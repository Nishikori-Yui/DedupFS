const API_BASE = "/api/v1";
const GROUP_PAGE_SIZE = 200;
const FILE_PAGE_SIZE = 120;
const GROUP_ROW_HEIGHT = 84;
const GROUP_OVERSCAN = 4;

const THUMB_MAX_CONCURRENT = 6;
const THUMB_POLL_BASE_MILLIS = 500;
const THUMB_POLL_MAX_MILLIS = 2400;
const THUMB_POLL_MAX_ATTEMPTS = 12;
const THUMB_REQUEST_RETRYABLE_STATUS = new Set([429, 503]);

class ApiError extends Error {
  constructor(message, { status = 0, detail = null, retryAfter = null } = {}) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
    this.retryAfter = retryAfter;
  }
}

const state = {
  groups: [],
  groupsCursor: null,
  groupsLoading: false,
  selectedGroupKey: null,
  files: [],
  filesCursor: null,
  filesLoading: false,
  filesLoadToken: 0,
  filesLoadingToken: 0,
  filesAbortController: null,
};

const thumbCache = new Map();
const thumbInflight = new Map();
const thumbQueue = [];
const thumbQueued = new Set();
let thumbRunningCount = 0;

const groupsViewport = document.getElementById("groups-viewport");
const groupsSpacer = document.getElementById("groups-spacer");
const groupsLayer = document.getElementById("groups-layer");
const groupsTemplate = document.getElementById("group-row-template");

const filesGrid = document.getElementById("files-grid");
const fileTemplate = document.getElementById("file-card-template");

const groupCountNode = document.getElementById("group-count");
const fileCountNode = document.getElementById("file-count");
const selectedGroupNode = document.getElementById("selected-group");
const filesNoteNode = document.getElementById("files-note");

const loadMoreGroupsBtn = document.getElementById("load-more-groups");
const refreshGroupsBtn = document.getElementById("refresh-groups");
const loadMoreFilesBtn = document.getElementById("load-more-files");

let thumbObserver = null;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatBytes(value) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let size = Number(value);
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  const digits = unit <= 1 ? 0 : 1;
  return `${size.toFixed(digits)} ${units[unit]}`;
}

function setGroupStats() {
  const totalFiles = state.groups.reduce((acc, item) => acc + Number(item.file_count || 0), 0);
  groupCountNode.textContent = `Groups: ${state.groups.length}`;
  fileCountNode.textContent = `Files: ${totalFiles}`;
}

async function parseResponseOrThrow(response) {
  const contentType = response.headers.get("content-type") || "";
  const isJson = contentType.includes("application/json");
  const payload = isJson ? await response.json() : null;

  if (response.ok) {
    return payload;
  }

  const detail = payload && typeof payload === "object" ? payload.detail : null;
  const retryAfter = payload && typeof payload === "object" ? payload.retry_after : null;
  const message = typeof detail === "string" ? detail : `Request failed (${response.status})`;
  throw new ApiError(message, { status: response.status, detail, retryAfter });
}

async function apiGet(path, init = {}) {
  const response = await fetch(`${API_BASE}${path}`, init);
  return parseResponseOrThrow(response);
}

async function apiPost(path, payload, init = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    ...init,
  });
  return parseResponseOrThrow(response);
}

function decodeCursor(cursor) {
  return cursor ? `&cursor=${encodeURIComponent(cursor)}` : "";
}

function retryDelayFromIso(value) {
  if (typeof value !== "string" || !value) {
    return null;
  }
  const dueAt = Date.parse(value);
  if (Number.isNaN(dueAt)) {
    return null;
  }
  const delay = dueAt - Date.now();
  return delay > 0 ? delay : 0;
}

function isRetryableApiError(error) {
  return error instanceof ApiError && THUMB_REQUEST_RETRYABLE_STATUS.has(error.status);
}

async function requestThumbnailWithBackoff(fileId) {
  const maxAttempts = 4;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      return await apiPost("/thumbs/request", {
        file_id: fileId,
        max_dimension: 256,
        output_format: "jpeg",
      });
    } catch (error) {
      if (!isRetryableApiError(error) || attempt === maxAttempts - 1) {
        throw error;
      }
      const base = 300 * 2 ** attempt;
      const jitter = Math.floor(Math.random() * 180);
      await sleep(base + jitter);
    }
  }

  throw new Error("thumbnail request exhausted retries");
}

async function loadGroups({ reset = false } = {}) {
  if (state.groupsLoading) {
    return;
  }
  state.groupsLoading = true;
  loadMoreGroupsBtn.disabled = true;

  if (reset) {
    state.groups = [];
    state.groupsCursor = null;
    state.selectedGroupKey = null;
    state.files = [];
    state.filesCursor = null;
    state.filesLoadToken += 1;
    state.filesLoading = false;
    state.filesLoadingToken = state.filesLoadToken;
    if (state.filesAbortController) {
      state.filesAbortController.abort();
      state.filesAbortController = null;
    }
    selectedGroupNode.textContent = "Select a duplicate group";
    filesNoteNode.textContent = "Thumbnail requests are on-demand and non-blocking.";
    renderFiles();
  }

  try {
    const payload = await apiGet(`/duplicates/groups?limit=${GROUP_PAGE_SIZE}${decodeCursor(state.groupsCursor)}`);
    state.groups.push(...payload.items);
    state.groupsCursor = payload.next_cursor;
    setGroupStats();
    renderGroupRows();

    if (!state.selectedGroupKey && state.groups.length > 0) {
      await selectGroup(state.groups[0].group_key);
    }
  } catch (error) {
    console.error(error);
  } finally {
    state.groupsLoading = false;
    loadMoreGroupsBtn.disabled = state.groupsCursor == null;
  }
}

function renderGroupRows() {
  const viewportHeight = groupsViewport.clientHeight;
  const scrollTop = groupsViewport.scrollTop;
  const total = state.groups.length;

  groupsSpacer.style.height = `${total * GROUP_ROW_HEIGHT}px`;

  const visibleCount = Math.max(1, Math.ceil(viewportHeight / GROUP_ROW_HEIGHT));
  const start = Math.max(0, Math.floor(scrollTop / GROUP_ROW_HEIGHT) - GROUP_OVERSCAN);
  const end = Math.min(total, start + visibleCount + GROUP_OVERSCAN * 2);

  groupsLayer.innerHTML = "";

  for (let index = start; index < end; index += 1) {
    const item = state.groups[index];
    const node = groupsTemplate.content.firstElementChild.cloneNode(true);
    node.style.top = `${index * GROUP_ROW_HEIGHT + 4}px`;
    node.dataset.groupKey = item.group_key;
    node.classList.toggle("active", item.group_key === state.selectedGroupKey);

    const groupKeyNode = node.querySelector(".group-key");
    const groupMetaNode = node.querySelector(".group-meta");
    groupKeyNode.textContent = item.group_key;
    groupMetaNode.textContent = `${item.file_count} files · total ${formatBytes(item.total_size_bytes)} · waste ${formatBytes(item.duplicate_waste_bytes)}`;

    node.addEventListener("click", () => {
      void selectGroup(item.group_key);
    });

    groupsLayer.appendChild(node);
  }
}

async function selectGroup(groupKey) {
  if (!groupKey) {
    return;
  }
  if (state.selectedGroupKey === groupKey && state.files.length > 0) {
    return;
  }

  state.selectedGroupKey = groupKey;
  state.files = [];
  state.filesCursor = null;
  state.filesLoadToken += 1;
  state.filesLoading = false;
  state.filesLoadingToken = state.filesLoadToken;

  if (state.filesAbortController) {
    state.filesAbortController.abort();
    state.filesAbortController = null;
  }

  selectedGroupNode.textContent = groupKey;
  filesNoteNode.textContent = "Loading files and requesting thumbnails lazily.";
  loadMoreFilesBtn.disabled = true;

  renderGroupRows();
  renderFiles();
  await loadFiles({ groupKey, loadToken: state.filesLoadToken });
}

async function loadFiles({ groupKey = state.selectedGroupKey, loadToken = state.filesLoadToken } = {}) {
  if (!groupKey) {
    return;
  }
  if (groupKey !== state.selectedGroupKey || loadToken !== state.filesLoadToken) {
    return;
  }
  if (state.filesLoading) {
    return;
  }

  state.filesLoading = true;
  state.filesLoadingToken = loadToken;
  loadMoreFilesBtn.disabled = true;

  const controller = new AbortController();
  state.filesAbortController = controller;

  try {
    const payload = await apiGet(
      `/duplicates/groups/${encodeURIComponent(groupKey)}/files?limit=${FILE_PAGE_SIZE}${decodeCursor(state.filesCursor)}`,
      { signal: controller.signal },
    );

    if (groupKey !== state.selectedGroupKey || loadToken !== state.filesLoadToken) {
      return;
    }

    state.files.push(...payload.items);
    state.filesCursor = payload.next_cursor;
    filesNoteNode.textContent = `Loaded ${state.files.length} files for selected group.`;
    renderFiles();
  } catch (error) {
    if (error && error.name === "AbortError") {
      return;
    }
    console.error(error);
    if (groupKey === state.selectedGroupKey && loadToken === state.filesLoadToken) {
      filesNoteNode.textContent = "Failed to load files for selected group.";
    }
  } finally {
    if (state.filesAbortController === controller) {
      state.filesAbortController = null;
    }
    if (state.filesLoadingToken === loadToken) {
      state.filesLoading = false;
      if (groupKey === state.selectedGroupKey && loadToken === state.filesLoadToken) {
        loadMoreFilesBtn.disabled = state.filesCursor == null;
      }
    }
  }
}

function renderFiles() {
  filesGrid.innerHTML = "";
  if (state.files.length === 0) {
    const placeholder = document.createElement("p");
    placeholder.textContent = "No files loaded.";
    placeholder.className = "panel-note";
    filesGrid.appendChild(placeholder);
    return;
  }

  for (const item of state.files) {
    const card = fileTemplate.content.firstElementChild.cloneNode(true);
    card.dataset.fileId = String(item.file_id);

    const pathNode = card.querySelector(".file-path");
    const sizeNode = card.querySelector(".file-size");
    const imageNode = card.querySelector(".thumb-image");
    const fallbackNode = card.querySelector(".thumb-fallback");

    pathNode.textContent = `${item.library_name}/${item.relative_path}`;
    sizeNode.textContent = `${formatBytes(item.size_bytes)} · id ${item.file_id}`;

    imageNode.dataset.fileId = String(item.file_id);
    fallbackNode.dataset.fileId = String(item.file_id);

    filesGrid.appendChild(card);
  }

  attachThumbObserver();
}

function attachThumbObserver() {
  if (thumbObserver) {
    thumbObserver.disconnect();
  }

  thumbObserver = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) {
          continue;
        }
        const imageNode = entry.target;
        const fileId = Number(imageNode.dataset.fileId);
        if (Number.isNaN(fileId)) {
          continue;
        }
        enqueueThumbnail(fileId);
        thumbObserver.unobserve(imageNode);
      }
    },
    {
      root: filesGrid,
      rootMargin: "120px",
      threshold: 0.1,
    },
  );

  const images = filesGrid.querySelectorAll(".thumb-image[data-file-id]");
  images.forEach((node) => thumbObserver.observe(node));
}

function enqueueThumbnail(fileId) {
  const cached = thumbCache.get(fileId);
  if (cached?.status === "ready" || cached?.status === "error") {
    return;
  }
  if (thumbInflight.has(fileId) || thumbQueued.has(fileId)) {
    return;
  }
  thumbQueue.push(fileId);
  thumbQueued.add(fileId);
  drainThumbnailQueue();
}

function drainThumbnailQueue() {
  while (thumbRunningCount < THUMB_MAX_CONCURRENT && thumbQueue.length > 0) {
    const fileId = thumbQueue.shift();
    thumbQueued.delete(fileId);

    thumbRunningCount += 1;
    void hydrateThumbnail(fileId)
      .catch((error) => {
        console.error(error);
      })
      .finally(() => {
        thumbRunningCount -= 1;
        drainThumbnailQueue();
      });
  }
}

function updateThumbnailUi(fileId, patch) {
  const imageNode = filesGrid.querySelector(`.thumb-image[data-file-id='${fileId}']`);
  const fallbackNode = filesGrid.querySelector(`.thumb-fallback[data-file-id='${fileId}']`);
  if (!imageNode || !fallbackNode) {
    return;
  }

  if (patch.contentUrl) {
    imageNode.src = patch.contentUrl;
    imageNode.classList.add("ready");
    fallbackNode.classList.add("ready");
    fallbackNode.textContent = "Ready";
    return;
  }

  if (patch.error) {
    fallbackNode.classList.add("error");
    fallbackNode.textContent = patch.error;
    return;
  }

  if (patch.label) {
    fallbackNode.textContent = patch.label;
  }
}

async function waitForThumbnail(thumbKey, fileId, initialSnapshot = null) {
  let snapshot = initialSnapshot;
  let pollMillis = THUMB_POLL_BASE_MILLIS;

  for (let attempt = 0; attempt < THUMB_POLL_MAX_ATTEMPTS; attempt += 1) {
    if (!snapshot) {
      await sleep(pollMillis);
      snapshot = await apiGet(`/thumbs/${encodeURIComponent(thumbKey)}`);
    }

    if (snapshot.status === "ready" && snapshot.content_url) {
      thumbCache.set(fileId, { status: "ready", contentUrl: snapshot.content_url });
      return snapshot.content_url;
    }

    if (snapshot.status === "failed") {
      const retryDelay = retryDelayFromIso(snapshot.retry_after);
      if (retryDelay !== null) {
        updateThumbnailUi(fileId, { label: "Retrying" });
        await sleep(Math.min(Math.max(retryDelay, THUMB_POLL_BASE_MILLIS), 5000));
        const requeued = await requestThumbnailWithBackoff(fileId);
        thumbKey = requeued.thumb_key;
        snapshot = requeued;
        pollMillis = THUMB_POLL_BASE_MILLIS;
        continue;
      }
      throw new Error(snapshot.error_code || "thumbnail failed");
    }

    snapshot = null;
    pollMillis = Math.min(pollMillis + 300, THUMB_POLL_MAX_MILLIS);
  }

  throw new Error("thumbnail timeout");
}

async function hydrateThumbnail(fileId) {
  const cached = thumbCache.get(fileId);
  if (cached?.status === "ready") {
    updateThumbnailUi(fileId, { contentUrl: cached.contentUrl });
    return;
  }
  if (cached?.status === "error") {
    updateThumbnailUi(fileId, { error: cached.message });
    return;
  }

  if (thumbInflight.has(fileId)) {
    await thumbInflight.get(fileId);
    return;
  }

  const task = (async () => {
    try {
      updateThumbnailUi(fileId, { label: "Queueing" });
      const requested = await requestThumbnailWithBackoff(fileId);

      if (requested.status === "ready" && requested.content_url) {
        thumbCache.set(fileId, { status: "ready", contentUrl: requested.content_url });
        updateThumbnailUi(fileId, { contentUrl: requested.content_url });
        return;
      }

      updateThumbnailUi(fileId, { label: "Rendering" });
      const contentUrl = await waitForThumbnail(requested.thumb_key, fileId, requested);
      updateThumbnailUi(fileId, { contentUrl });
    } catch (error) {
      const message = error instanceof Error ? error.message : "thumbnail unavailable";
      thumbCache.set(fileId, { status: "error", message });
      const display = message.includes("timeout") ? "Timeout" : "Error";
      updateThumbnailUi(fileId, { error: display });
    } finally {
      thumbInflight.delete(fileId);
    }
  })();

  thumbInflight.set(fileId, task);
  await task;
}

groupsViewport.addEventListener("scroll", () => {
  renderGroupRows();

  const nearBottom =
    groupsViewport.scrollTop + groupsViewport.clientHeight >= groupsViewport.scrollHeight - GROUP_ROW_HEIGHT * 1.5;
  if (nearBottom && state.groupsCursor && !state.groupsLoading) {
    void loadGroups();
  }
});

loadMoreGroupsBtn.addEventListener("click", () => {
  void loadGroups();
});

refreshGroupsBtn.addEventListener("click", () => {
  void loadGroups({ reset: true });
});

loadMoreFilesBtn.addEventListener("click", () => {
  void loadFiles();
});

void loadGroups({ reset: true });
