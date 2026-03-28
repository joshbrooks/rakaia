import { QueryClient, QueryObserver } from "https://esm.sh/@tanstack/query-core@5.39.1";
import { persistQueryClient } from "https://esm.sh/@tanstack/query-persist-client-core@5.39.1";
import { createIDBPersister } from "https://esm.sh/@tanstack/query-persist-client-indexeddb@5.39.1";

async function bootstrapTranslationsApp() {
  const container = document.getElementById("translations-app");
  if (!container) {
    return;
  }

  const endpoints = {
    translations: container.dataset.apiTranslationsUrl,
    create: container.dataset.apiCreateUrl,
    sse: container.dataset.sseUrl,
  };

  const elements = {
    addButton: document.getElementById("add-translation-btn"),
    modal: document.getElementById("translation-modal"),
    modalTitle: document.getElementById("modal-title"),
    modalClose: document.querySelector(".modal .close"),
    modalCancel: document.getElementById("cancel-translation-btn"),
    modalSave: document.getElementById("save-translation-btn"),
    msgid: document.getElementById("msgid-input"),
    msgstr: document.getElementById("msgstr-input"),
    langcode: document.getElementById("langcode-input"),
    domain: document.getElementById("domain-input"),
    msgctxt: document.getElementById("msgctxt-input"),
    langFilter: document.getElementById("langcode-filter"),
    msgidFilter: document.getElementById("msgid-filter"),
    applyFilters: document.getElementById("apply-filters-btn"),
    clearFilters: document.getElementById("clear-filters-btn"),
    tbody: document.getElementById("translations-tbody"),
    loading: document.getElementById("loading-indicator"),
    noResults: document.getElementById("no-results"),
    activityList: document.getElementById("recent-activity"),
  };

  const state = {
    filters: {
      langcode: elements.langFilter?.value ?? "",
      msgid: elements.msgidFilter?.value ?? "",
    },
    editing: null,
    activityIds: new Set(),
  };

  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: 60_000,
        gcTime: 1000 * 60 * 60,
        retry: 1,
        refetchOnWindowFocus: false,
      },
    },
  });

  try {
    await persistQueryClient({
      queryClient,
      persister: createIDBPersister({
        dbName: "translations-cache",
        storeName: "tanstack",
      }),
      maxAge: 1000 * 60 * 60 * 24, // 24h
      dehydrateOptions: {
        shouldDehydrateQuery: (query) => query.queryKey[0] === "translations",
      },
    });
  } catch (error) {
    console.warn("Failed to enable IndexedDB persistence", error);
  }

  const translationsObserver = new QueryObserver(queryClient, {
    queryKey: ["translations", { ...state.filters }],
    queryFn: ({ queryKey }) => fetchTranslations(queryKey[1], endpoints.translations),
  });

  translationsObserver.subscribe((result) => renderTranslations(result, elements));

  function refreshQuery(filters = state.filters) {
    translationsObserver.setOptions({
      queryKey: ["translations", { ...filters }],
      queryFn: ({ queryKey }) => fetchTranslations(queryKey[1], endpoints.translations),
    });
  }

  elements.applyFilters?.addEventListener("click", () => {
    state.filters = {
      langcode: elements.langFilter?.value ?? "",
      msgid: elements.msgidFilter?.value ?? "",
    };
    refreshQuery();
  });

  elements.clearFilters?.addEventListener("click", () => {
    if (elements.langFilter) elements.langFilter.value = "";
    if (elements.msgidFilter) elements.msgidFilter.value = "";
    state.filters = { langcode: "", msgid: "" };
    refreshQuery();
  });

  elements.addButton?.addEventListener("click", () => openModal());
  elements.modalClose?.addEventListener("click", closeModal);
  elements.modalCancel?.addEventListener("click", closeModal);
  window.addEventListener("click", (event) => {
    if (event.target === elements.modal) {
      closeModal();
    }
  });
  elements.modalSave?.addEventListener("click", () => saveTranslation(endpoints.create));

  setupSSE(endpoints.sse);
  hydrateInitialActivity();

  function renderTranslations(result, ui) {
    const { status, data, error, isFetching } = result;
    if (ui.loading) {
      ui.loading.style.display = status === "loading" || isFetching ? "block" : "none";
    }

    if (error && ui.tbody) {
      ui.tbody.innerHTML = `<tr><td colspan="6" style="text-align:center;color:var(--danger-color);">${error.message}</td></tr>`;
      return;
    }

    if (!data) {
      return;
    }

    const translations = data.translations ?? [];
    if (ui.noResults) {
      ui.noResults.style.display = translations.length === 0 ? "block" : "none";
    }

    if (!ui.tbody) {
      return;
    }

    ui.tbody.innerHTML = "";
    translations.forEach((translation) => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${escapeHtml(translation.msgid)}</td>
        <td>${escapeHtml(translation.msgstr || "")}</td>
        <td><span class="lang-badge">${translation.langcode}</span></td>
        <td>${escapeHtml(translation.domain || "")}</td>
        <td>${escapeHtml(translation.msgctxt || "")}</td>
        <td><button class="btn btn-sm btn-outline" data-role="edit">Edit</button></td>
      `;
      const editBtn = row.querySelector('[data-role="edit"]');
      editBtn?.addEventListener("click", () => openModal(translation));
      ui.tbody.appendChild(row);
    });
  }

  async function fetchTranslations(filters, url) {
    const params = new URLSearchParams();
    if (filters.langcode) params.append("langcode", filters.langcode);
    if (filters.msgid) params.append("msgid", filters.msgid);
    const response = await fetch(parametrize(url, params));
    if (!response.ok) {
      throw new Error("Unable to load translations");
    }
    return response.json();
  }

  function parametrize(url, params) {
    const qs = params.toString();
    if (!qs) return url;
    return `${url}?${qs}`;
  }

  function openModal(translation = null) {
    state.editing = translation;
    if (!elements.modal || !elements.modalTitle) return;
    if (translation) {
      elements.modalTitle.textContent = "Edit Translation";
      elements.msgid.value = translation.msgid ?? "";
      elements.msgstr.value = translation.msgstr ?? "";
      elements.langcode.value = translation.langcode ?? "";
      elements.domain.value = translation.domain ?? "";
      elements.msgctxt.value = translation.msgctxt ?? "";
    } else {
      elements.modalTitle.textContent = "Add Translation";
      document.getElementById("translation-form")?.reset();
    }
    elements.modal.style.display = "block";
  }

  function closeModal() {
    if (elements.modal) {
      elements.modal.style.display = "none";
    }
    state.editing = null;
  }

  async function saveTranslation(createUrl) {
    const payload = {
      msgid: elements.msgid.value.trim(),
      msgstr: elements.msgstr.value.trim(),
      langcode: elements.langcode.value,
      domain: elements.domain.value.trim() || undefined,
      msgctxt: elements.msgctxt.value.trim() || undefined,
      url: window.location.pathname,
    };

    if (!payload.msgid || !payload.langcode) {
      alert("Message ID and Language are required");
      return;
    }

    try {
      const response = await fetch(createUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCookie("csrftoken"),
        },
        body: JSON.stringify(payload),
      });

      const data = await response.json();
      if (!response.ok || !data.success) {
        throw new Error(data.error || "Unable to save translation");
      }

      closeModal();
      queryClient.invalidateQueries({ queryKey: ["translations"] });
      showNotification("Translation saved", "success");
    } catch (error) {
      console.error(error);
      alert(error.message || "Error saving translation");
    }
  }

  function setupSSE(url) {
    if (!url) return;
    let eventSource = new EventSource(url);

    eventSource.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.stream) {
          addActivityItem(payload.stream);
          queryClient.invalidateQueries({ queryKey: ["translations"] });
        }
      } catch (error) {
        console.warn("Unable to parse SSE payload", error);
      }
    };

    eventSource.onerror = () => {
      console.log("SSE disconnected, retrying in 5s");
      eventSource.close();
      setTimeout(() => setupSSE(url), 5000);
    };
  }

  function hydrateInitialActivity() {
    const script = document.getElementById("initial-translation-activity");
    if (!script) return;
    try {
      const items = JSON.parse(script.textContent);
      items.forEach((item) => {
        state.activityIds.add(item.id);
      });
    } catch (error) {
      console.warn("Unable to hydrate activity cache", error);
    }
  }

  function addActivityItem(stream) {
    if (!elements.activityList || state.activityIds.has(stream.id)) {
      return;
    }

    state.activityIds.add(stream.id);
    const newItem = document.createElement("div");
    newItem.className = "activity-item";
    newItem.innerHTML = `
      <div class="activity-header">
        <span class="activity-user">${escapeHtml(stream.user || "Unknown")}</span>
        <span class="activity-action">${escapeHtml(stream.action || "")}</span>
        <span class="activity-lang">[${escapeHtml(stream.langcode || "")}]
        </span>
        <span class="activity-time">${formatTime(stream.created_at)}</span>
      </div>
      <div class="activity-content">
        <strong>${escapeHtml(stream.translatable?.msgid || "")}</strong>
        ${stream.translatable?.msgstr ? ` → ${escapeHtml(stream.translatable.msgstr)}` : ""}
      </div>
      <div class="activity-url">${escapeHtml(stream.url || "")}</div>
    `;

    elements.activityList.prepend(newItem);

    while (elements.activityList.children.length > 20) {
      const removed = elements.activityList.lastElementChild;
      if (removed) {
        elements.activityList.removeChild(removed);
      }
    }
  }

  function formatTime(value) {
    if (!value) return new Date().toLocaleTimeString();
    try {
      return new Date(value).toLocaleTimeString();
    } catch (error) {
      return value;
    }
  }

  function showNotification(message, type = "info") {
    console.log(`${type}: ${message}`);
  }

  function escapeHtml(text) {
    const div = document.createElement("div");
    div.textContent = text ?? "";
    return div.innerHTML;
  }

  function getCookie(name) {
    const cookies = document.cookie ? document.cookie.split(";") : [];
    for (const cookie of cookies) {
      const trimmed = cookie.trim();
      if (trimmed.startsWith(name + "=")) {
        return decodeURIComponent(trimmed.substring(name.length + 1));
      }
    }
    return "";
  }
}

bootstrapTranslationsApp();
