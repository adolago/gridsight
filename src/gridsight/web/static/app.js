const state = {
  offset: 0,
  limit: 50,
  total: 0,
  hasNext: false,
};

const form = document.getElementById("filters-form");
const resetButton = document.getElementById("reset-filters");
const playsBody = document.getElementById("plays-body");
const resultMeta = document.getElementById("result-meta");
const prevPageButton = document.getElementById("prev-page");
const nextPageButton = document.getElementById("next-page");
const pageLabel = document.getElementById("page-label");

function option(select, value, label) {
  const item = document.createElement("option");
  item.value = value;
  item.textContent = label;
  select.appendChild(item);
}

function populateSelect(id, values) {
  const select = document.getElementById(id);
  select.innerHTML = "";
  option(select, "", "All");

  values.forEach((value) => {
    option(select, String(value), String(value));
  });
}

function toNumber(value) {
  if (value === "" || value == null) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function fmt(value, digits = 2) {
  if (value == null || Number.isNaN(value)) {
    return "-";
  }
  return Number(value).toFixed(digits);
}

function setTableMessage(text, className = "empty") {
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 13;
  cell.className = className;
  cell.textContent = text;
  row.appendChild(cell);

  playsBody.innerHTML = "";
  playsBody.appendChild(row);
}

function updatePagination() {
  const page = Math.floor(state.offset / state.limit) + 1;
  const totalPages = Math.max(1, Math.ceil(state.total / state.limit));

  pageLabel.textContent = `Page ${page} / ${totalPages}`;
  prevPageButton.disabled = state.offset <= 0;
  nextPageButton.disabled = !state.hasNext;
}

function readParams() {
  const params = new URLSearchParams();
  const valueOf = (id) => document.getElementById(id).value.trim();

  const repeatable = ["season", "week", "posteam", "defteam", "play_type", "down"];
  repeatable.forEach((name) => {
    const value = valueOf(name);
    if (value !== "") {
      params.append(name, value);
    }
  });

  const textFields = ["game_id", "description_search"];
  textFields.forEach((name) => {
    const value = valueOf(name);
    if (value !== "") {
      params.set(name, value);
    }
  });

  const numericFields = ["ydstogo_min", "ydstogo_max", "epa_min", "epa_max"];
  numericFields.forEach((name) => {
    const parsed = toNumber(valueOf(name));
    if (parsed != null) {
      params.set(name, String(parsed));
    }
  });

  const sortBy = valueOf("sort_by") || "season";
  const sortDir = valueOf("sort_dir") || "desc";
  const limit = toNumber(valueOf("limit")) ?? 50;

  params.set("sort_by", sortBy);
  params.set("sort_dir", sortDir);
  params.set("limit", String(limit));
  params.set("offset", String(state.offset));

  state.limit = limit;
  return params;
}

function renderRows(items) {
  if (!items.length) {
    setTableMessage("No plays match the current filters.");
    return;
  }

  const fragment = document.createDocumentFragment();
  items.forEach((play) => {
    const row = document.createElement("tr");

    const cells = [
      play.season,
      play.week,
      play.game_id,
      play.play_id,
      play.posteam ?? "-",
      play.defteam ?? "-",
      play.play_type ?? "-",
      play.down ?? "-",
      play.ydstogo ?? "-",
      play.yards_gained ?? "-",
      play.epa != null ? fmt(play.epa, 3) : "-",
      play.wpa != null ? fmt(play.wpa, 3) : "-",
      play.description ?? "-",
    ];

    cells.forEach((value, index) => {
      const cell = document.createElement("td");
      cell.textContent = String(value);
      if (index === 12) {
        cell.className = "desc";
      }
      row.appendChild(cell);
    });

    fragment.appendChild(row);
  });

  playsBody.innerHTML = "";
  playsBody.appendChild(fragment);
}

async function fetchFilterOptions() {
  const response = await fetch("/v1/plays/filter-options");
  if (!response.ok) {
    throw new Error("Failed to load filter options.");
  }

  const data = await response.json();
  populateSelect("season", data.seasons || []);
  populateSelect("week", data.weeks || []);
  populateSelect("posteam", data.posteams || []);
  populateSelect("defteam", data.defteams || []);
  populateSelect("play_type", data.play_types || []);
  populateSelect("down", data.downs || []);
}

async function fetchPlays() {
  setTableMessage("Loading plays...");
  resultMeta.textContent = "Loading...";

  try {
    const params = readParams();
    const response = await fetch(`/v1/plays?${params.toString()}`);

    if (!response.ok) {
      let detail = `HTTP ${response.status}`;
      try {
        const body = await response.json();
        detail = body.detail ?? detail;
      } catch {
        // Ignore JSON parse errors for error payloads.
      }
      throw new Error(detail);
    }

    const data = await response.json();

    state.total = data.total;
    state.limit = data.limit;
    state.offset = data.offset;
    state.hasNext = data.has_next;

    renderRows(data.items);

    const shownFrom = data.total === 0 ? 0 : data.offset + 1;
    const shownTo = Math.min(data.offset + data.items.length, data.total);
    resultMeta.textContent = `${shownFrom}-${shownTo} of ${data.total} plays`;

    updatePagination();
  } catch (error) {
    setTableMessage(`Failed to load plays: ${error.message}`, "error");
    resultMeta.textContent = "Error";
    state.total = 0;
    state.hasNext = false;
    updatePagination();
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  state.offset = 0;
  fetchPlays();
});

resetButton.addEventListener("click", () => {
  form.reset();
  state.offset = 0;
  fetchPlays();
});

prevPageButton.addEventListener("click", () => {
  state.offset = Math.max(0, state.offset - state.limit);
  fetchPlays();
});

nextPageButton.addEventListener("click", () => {
  if (!state.hasNext) {
    return;
  }
  state.offset += state.limit;
  fetchPlays();
});

(async () => {
  try {
    await fetchFilterOptions();
  } catch (error) {
    setTableMessage(`Failed to initialize filters: ${error.message}`, "error");
    resultMeta.textContent = "Initialization error";
    return;
  }

  await fetchPlays();
})();
