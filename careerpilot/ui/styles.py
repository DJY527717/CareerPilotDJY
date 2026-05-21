from __future__ import annotations

import streamlit as st


def inject_global_styles() -> None:
    st.markdown(
        """
        <style>
        /* base */
        :root {
          --cp-bg: #FBFAF7;
          --cp-bg-soft: #F6F4EF;
          --cp-surface: rgba(255,255,255,.78);
          --cp-surface-solid: #FFFFFF;
          --cp-text: #1D1D1F;
          --cp-text-2: #515154;
          --cp-muted: #86868B;
          --cp-accent: #6F9F8A;
          --cp-accent-soft: #EAF3EE;
          --cp-accent-faint: #F3F8F5;
          --cp-primary: #6F9F8A;
          --cp-primary-hover: #5F8D7A;
          --cp-primary-soft: rgba(111,159,138,.12);
          --cp-ink: #1D1D1F;
          --cp-control-bg: rgba(255,255,255,.74);
          --cp-control-hover: rgba(255,255,255,.92);
          --cp-disabled-bg: rgba(29,29,31,.06);
          --cp-disabled-text: rgba(29,29,31,.34);
          --cp-line: rgba(29,29,31,.06);
          --cp-shadow-sheet: 0 10px 28px rgba(0,0,0,.028);
          --cp-shadow-soft: 0 6px 18px rgba(0,0,0,.024);
          --cp-radius-sheet: 24px;
          --cp-radius-card: 20px;
          --cp-radius-control: 14px;
          --cp-radius-pill: 999px;
          --cp-sidebar-width: 300px;
          --cp-font: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "PingFang SC", "Microsoft YaHei", sans-serif;
          --cp-danger: #8B6B62;
          --cp-danger-soft: #F6EFEC;
          --cp-warning: #8F765A;
          --cp-warning-soft: #F7F2EA;
          --cp-info: #6F7F91;
          --cp-info-soft: #EEF1F3;
        }

        * {
          box-sizing: border-box;
          letter-spacing: 0 !important;
        }

        html,
        body,
        .stApp,
        [data-testid="stAppViewContainer"] {
          overflow-x: hidden !important;
        }

        html,
        body,
        .stApp {
          min-height: 100%;
          background:
            radial-gradient(circle at 20% 0%, rgba(234,243,238,.70), transparent 32rem),
            linear-gradient(180deg, #FBFAF7 0%, #F8F6F1 100%) !important;
          color: var(--cp-text) !important;
          font-family: var(--cp-font) !important;
          text-rendering: geometricPrecision;
        }

        #MainMenu,
        [data-testid="stDeployButton"],
        [data-testid="stAppDeployButton"],
        [data-testid="stToolbar"],
        footer {
          display: none !important;
          visibility: hidden !important;
        }

        header[data-testid="stHeader"],
        [data-testid="stToolbar"] {
          background: transparent !important;
          border: 0 !important;
          box-shadow: none !important;
        }

        [data-testid="stAppViewContainer"],
        section[data-testid="stMain"],
        div[data-testid="stMainBlockContainer"] {
          background: transparent !important;
        }

        .block-container {
          max-width: min(1180px, calc(100vw - 2rem));
          padding: 1.8rem 2rem 3.6rem !important;
        }

        [data-testid="stVerticalBlock"] {
          gap: 1rem !important;
        }

        h1, h2, h3, h4, h5, h6,
        [data-testid="stMarkdownContainer"] h1,
        [data-testid="stMarkdownContainer"] h2,
        [data-testid="stMarkdownContainer"] h3 {
          color: var(--cp-text) !important;
          font-family: var(--cp-font) !important;
          font-weight: 720 !important;
          line-height: 1.14 !important;
        }

        [data-testid="stMarkdownContainer"] p,
        [data-testid="stMarkdownContainer"] li,
        [data-testid="stCaptionContainer"],
        label,
        p {
          color: var(--cp-text-2) !important;
          font-family: var(--cp-font) !important;
          font-size: 15px;
          font-weight: 430;
          line-height: 1.68;
        }

        small,
        [data-testid="stCaptionContainer"],
        [data-testid="stMarkdownContainer"] small {
          color: var(--cp-muted) !important;
          font-size: 13px !important;
          line-height: 1.5 !important;
        }

        a {
          color: var(--cp-primary) !important;
          text-decoration-color: rgba(29,29,31,.22) !important;
        }

        /* app shell */
        .cp-workspace-scope,
        .cp-shell-spacer {
          display: none !important;
        }

        .cp-topbar {
          display: flex;
          align-items: flex-end;
          justify-content: space-between;
          gap: 20px;
          min-height: 80px;
          margin: 0 0 18px;
          padding: 2px 2px 16px;
          border-bottom: 1px solid rgba(29,29,31,.045);
          background: transparent !important;
        }

        .cp-topbar-main {
          min-width: 0;
        }

        .cp-topbar-eyebrow,
        .cp-workspace-eyebrow,
        .cp-subsection-label,
        .cp-field-label,
        .cp-sidebar-block-title,
        .cp-sidebar-toolbox-title {
          display: block;
          color: var(--cp-muted) !important;
          font-size: 12px !important;
          font-weight: 650 !important;
          line-height: 1.35 !important;
          text-transform: none;
        }

        .cp-topbar h1 {
          margin: 0;
          color: var(--cp-text) !important;
          font-size: clamp(30px, 3vw, 40px);
          font-weight: 740 !important;
          letter-spacing: 0 !important;
        }

        .cp-topbar p {
          max-width: 760px;
          margin: 8px 0 0 !important;
          color: var(--cp-muted) !important;
          font-size: 15px !important;
          line-height: 1.65 !important;
        }

        .cp-topbar-chips,
        .cp-chip-row,
        .cp-risk-tag-row,
        .cp-job-card-meta {
          display: flex;
          align-items: center;
          flex-wrap: wrap;
          gap: 8px;
        }

        .cp-topbar-chips {
          justify-content: flex-end;
          padding-bottom: 3px;
        }

        .cp-topbar-chip,
        .cp-risk-tag,
        .cp-table-count {
          display: inline-flex;
          align-items: center;
          min-height: 30px;
          padding: 5px 10px;
          border: 1px solid var(--cp-line);
          border-radius: var(--cp-radius-pill);
          background: rgba(255,255,255,.56);
          color: var(--cp-text-2) !important;
          font-size: 12px;
          font-weight: 560;
          white-space: nowrap;
        }

        /* auth page */
        .cp-auth-scope {
          display: none !important;
        }

        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) {
          max-width: 1040px;
          padding-top: 40px !important;
          padding-bottom: 40px !important;
        }

        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) .stColumn {
          align-content: start;
        }

        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) .stColumn [data-testid="stVerticalBlock"]:has(> [data-testid="stElementContainer"] .cp-auth-card) {
          width: 100%;
          max-width: 450px;
          margin: 0 auto;
          padding: 24px !important;
          border: 1px solid rgba(29,29,31,.06) !important;
          border-radius: 22px !important;
          background: rgba(255,255,255,.78) !important;
          box-shadow: 0 8px 22px rgba(0,0,0,.024) !important;
          align-items: stretch !important;
        }

        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) .stColumn [data-testid="stVerticalBlock"]:has(> [data-testid="stElementContainer"] .cp-auth-card) [data-testid="stElementContainer"],
        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) .stColumn [data-testid="stVerticalBlock"]:has(> [data-testid="stElementContainer"] .cp-auth-card) [data-testid="stMarkdown"],
        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) .stColumn [data-testid="stVerticalBlock"]:has(> [data-testid="stElementContainer"] .cp-auth-card) [data-testid="stMarkdownContainer"],
        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) .stColumn [data-testid="stVerticalBlock"]:has(> [data-testid="stElementContainer"] .cp-auth-card) .cp-auth-card {
          width: 100% !important;
        }

        .cp-auth-story {
          max-width: 520px;
          padding: 8px 0;
        }

        .cp-auth-brand-row {
          display: inline-flex;
          align-items: center;
          gap: 12px;
          margin-bottom: 26px;
        }

        .cp-logo-mark,
        .cp-auth-logo {
          display: inline-grid;
          place-items: center;
          width: 36px;
          height: 36px;
          border: 1px solid rgba(111,159,138,.24);
          border-radius: 12px;
          background: linear-gradient(180deg, var(--cp-accent-faint), var(--cp-accent-soft));
          color: var(--cp-text) !important;
          font-size: 12px;
          font-weight: 760;
          box-shadow: var(--cp-shadow-soft);
        }

        .cp-auth-product {
          color: var(--cp-text) !important;
          font-size: 16px;
          font-weight: 700;
          line-height: 1.2;
        }

        .cp-auth-story h1 {
          max-width: 540px;
          margin: 0;
          color: var(--cp-text) !important;
          font-size: clamp(36px, 3.9vw, 48px);
          font-weight: 760 !important;
          line-height: 1.06 !important;
        }

        .cp-auth-story p {
          max-width: 520px;
          margin: 16px 0 0 !important;
          color: var(--cp-text-2) !important;
          font-size: 16px !important;
          line-height: 1.72 !important;
        }

        .cp-auth-feature-list {
          display: grid;
          gap: 10px;
          max-width: 460px;
          margin-top: 16px;
        }

        .cp-auth-steps-line {
          margin-top: 14px;
          color: var(--cp-text) !important;
          font-size: 14px;
          font-weight: 660;
          line-height: 1.45;
        }

        .cp-auth-feature,
        .cp-auth-value-step {
          display: grid;
          grid-template-columns: 26px minmax(0, 1fr);
          gap: 12px;
          align-items: start;
          padding: 1px 0;
        }

        .cp-auth-feature-icon,
        .cp-auth-value-step em {
          display: inline-grid;
          place-items: center;
          width: 21px;
          height: 21px;
          margin-top: 2px;
          border-radius: var(--cp-radius-pill);
          background: var(--cp-accent-faint);
          color: var(--cp-accent) !important;
          font-size: 11px;
          font-style: normal;
          font-weight: 620;
        }

        .cp-auth-feature strong,
        .cp-auth-value-step strong {
          display: block;
          color: var(--cp-text) !important;
          font-size: 15px;
          font-weight: 670;
          line-height: 1.35;
        }

        .cp-auth-feature span,
        .cp-auth-value-step span {
          display: block;
          margin-top: 2px;
          color: var(--cp-muted) !important;
          font-size: 13px;
          line-height: 1.45;
        }

        .cp-auth-sheet,
        .cp-auth-card {
          border: 0 !important;
          padding: 0 !important;
          background: transparent !important;
          box-shadow: none !important;
          max-width: 450px;
          margin: 0 auto 10px;
        }

        .cp-auth-sheet-kicker,
        .cp-auth-card-kicker {
          margin-bottom: 9px;
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-weight: 650;
        }

        .cp-auth-sheet h2,
        .cp-auth-sheet-head h2,
        .cp-auth-card h2,
        .cp-auth-card-title {
          margin: 0 !important;
          color: var(--cp-text) !important;
          font-size: clamp(30px, 3vw, 34px) !important;
          font-weight: 720 !important;
          line-height: 1.14 !important;
        }

        .cp-auth-sheet-copy,
        .cp-auth-card-copy,
        .cp-auth-form-note {
          margin: 10px 0 0 !important;
          color: var(--cp-muted) !important;
          font-size: 14px !important;
          line-height: 1.62 !important;
        }

        .cp-auth-tabs-gap {
          height: 10px;
        }

        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) [data-testid="stRadio"] {
          max-height: 44px;
          margin-bottom: 12px !important;
        }

        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) [data-testid="stRadio"] label,
        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) label[data-baseweb="radio"] {
          min-height: 34px !important;
          padding: 0 18px !important;
        }

        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) [data-testid="stTextInput"] input {
          min-height: 44px !important;
          height: 46px !important;
        }

        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) [data-testid="stFormSubmitButton"] button {
          min-height: 48px !important;
          height: 48px !important;
        }

        .cp-auth-showcase,
        .cp-auth-hero,
        .cp-auth-spotlight,
        .cp-auth-orbit,
        .cp-weekly-focus,
        .cp-auth-showcase-note {
          display: none !important;
        }

        body:has(.cp-auth-scope) [data-testid="stSidebar"],
        body:has(.cp-auth-scope) [data-testid="collapsedControl"] {
          display: none !important;
        }

        /* sidebar */
        section[data-testid="stSidebar"],
        [data-testid="stSidebar"] {
          width: var(--cp-sidebar-width) !important;
          min-width: var(--cp-sidebar-width) !important;
          padding: 12px 0 12px 12px !important;
          border-right: 1px solid var(--cp-line) !important;
          background: rgba(251,250,247,.82) !important;
          box-shadow: none !important;
        }

        section[data-testid="stSidebar"] > div:first-child,
        [data-testid="stSidebar"] > div:first-child {
          width: var(--cp-sidebar-width) !important;
          min-width: var(--cp-sidebar-width) !important;
          background: transparent !important;
          border: 0 !important;
          box-shadow: none !important;
        }

        [data-testid="stSidebarContent"] {
          width: calc(var(--cp-sidebar-width) - 12px) !important;
          min-width: calc(var(--cp-sidebar-width) - 12px) !important;
          height: calc(100vh - 24px) !important;
          max-height: calc(100vh - 24px) !important;
          margin: 0 !important;
          padding: 18px 14px 22px !important;
          border: 1px solid var(--cp-line) !important;
          border-radius: 26px !important;
          background: rgba(255,255,255,.52) !important;
          box-shadow: 0 8px 24px rgba(0,0,0,.025) !important;
          backdrop-filter: blur(16px) saturate(1.08);
          -webkit-backdrop-filter: blur(16px) saturate(1.08);
          overflow-y: auto !important;
          overflow-x: hidden !important;
          scrollbar-width: thin !important;
          scrollbar-color: rgba(29,29,31,.22) transparent !important;
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar {
          width: 6px;
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar-track {
          background: transparent;
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar-thumb {
          border-radius: var(--cp-radius-pill);
          background: rgba(29,29,31,.18);
        }

        section[data-testid="stSidebar"][aria-expanded="false"],
        [data-testid="stSidebar"][aria-expanded="false"] {
          width: 0 !important;
          min-width: 0 !important;
          padding: 0 !important;
          border: 0 !important;
          overflow: visible !important;
        }

        section[data-testid="stSidebar"][aria-expanded="false"] > div,
        section[data-testid="stSidebar"][aria-expanded="false"] [data-testid="stSidebarContent"],
        [data-testid="stSidebar"][aria-expanded="false"] > div,
        [data-testid="stSidebar"][aria-expanded="false"] [data-testid="stSidebarContent"] {
          width: 0 !important;
          min-width: 0 !important;
          padding: 0 !important;
          margin: 0 !important;
          border: 0 !important;
          pointer-events: none !important;
        }

        [data-testid="collapsedControl"] {
          z-index: 999999 !important;
        }

        [data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
          gap: .62rem !important;
        }

        .cp-sidebar-brand,
        .cp-sidebar-status,
        .cp-sidebar-status-card {
          margin: 0 0 12px !important;
          padding: 4px 4px 8px !important;
          border: 0 !important;
          background: transparent !important;
          box-shadow: none !important;
        }

        .cp-sidebar-brand-row {
          display: flex;
          align-items: center;
          gap: 11px;
        }

        .cp-sidebar-title,
        .cp-sidebar-brand strong {
          display: block;
          color: var(--cp-text) !important;
          font-size: 17px;
          font-weight: 740;
          line-height: 1.18;
        }

        .cp-sidebar-kicker,
        .cp-sidebar-copy,
        .cp-sidebar-meta,
        .cp-sidebar-brand em {
          display: block;
          margin-top: 5px;
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-style: normal;
          font-weight: 520;
          line-height: 1.45;
          overflow: hidden;
          display: -webkit-box;
          -webkit-line-clamp: 2;
          -webkit-box-orient: vertical;
        }

        .cp-sidebar-status {
          display: grid;
          gap: 8px;
          padding-top: 10px !important;
        }

        .cp-sidebar-status-row {
          display: grid;
          gap: 2px !important;
          min-height: 0 !important;
          padding: 7px 9px !important;
          border-radius: 14px;
          background: rgba(246,244,239,.52);
        }

        .cp-sidebar-status-row span {
          color: var(--cp-muted) !important;
          font-size: 11px;
          font-weight: 600;
          line-height: 1.3;
        }

        .cp-sidebar-status-row strong {
          overflow: hidden;
          color: var(--cp-text) !important;
          font-size: 13px;
          font-weight: 650;
          line-height: 1.35;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        .cp-sidebar-section-label,
        .cp-sidebar-mini-title {
          margin: 12px 8px 7px;
          color: var(--cp-muted) !important;
          font-size: 11px !important;
          font-weight: 680 !important;
          line-height: 1.3 !important;
        }

        .cp-sidebar-context-card {
          display: grid;
          gap: 7px;
          margin: 12px 0 10px !important;
          padding: 12px !important;
          border: 1px solid rgba(29,29,31,.055) !important;
          border-radius: 20px !important;
          background: rgba(255,255,255,.52) !important;
          box-shadow: none !important;
        }

        .cp-sidebar-context-row {
          display: grid;
          grid-template-columns: 44px minmax(0, 1fr);
          align-items: center;
          gap: 8px;
          min-height: 25px;
        }

        .cp-sidebar-context-row span {
          color: var(--cp-muted) !important;
          font-size: 11px !important;
          font-weight: 620 !important;
        }

        .cp-sidebar-context-row strong {
          overflow: hidden;
          color: var(--cp-text) !important;
          font-size: 12px !important;
          font-weight: 650 !important;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        .cp-sidebar-user-line {
          margin: 0 8px 10px;
          color: var(--cp-muted) !important;
          font-size: 11px !important;
          line-height: 1.4 !important;
        }

        .cp-sidebar-divider {
          height: 1px;
          margin: 16px 0 12px;
          background: rgba(29,29,31,.055);
        }

        .cp-sidebar-footer {
          margin-top: 12px;
          padding-top: 10px;
          border-top: 1px solid rgba(29,29,31,.055);
        }

        .cp-sidebar-block-title,
        .cp-sidebar-toolbox-title {
          margin: 16px 8px 6px;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] {
          padding: 0 !important;
          border: 0 !important;
          background: transparent !important;
        }

        [data-testid="stSidebar"] [role="radiogroup"] {
          display: grid !important;
          gap: 2px !important;
          background: transparent !important;
        }

        [data-testid="stSidebar"] [role="radiogroup"] svg,
        [data-testid="stSidebar"] [data-baseweb="radio"] > div:first-child {
          display: none !important;
        }

        [data-testid="stSidebar"] [role="radiogroup"] input[type="radio"] {
          position: absolute !important;
          inset: 0 !important;
          width: 100% !important;
          height: 100% !important;
          margin: 0 !important;
          opacity: 0 !important;
          cursor: pointer !important;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label,
        [data-testid="stSidebar"] label[data-baseweb="radio"],
        [data-testid="stSidebar"] [role="radio"] {
          position: relative !important;
          display: flex !important;
          align-items: center !important;
          min-height: 36px !important;
          margin: 0 !important;
          padding: 0 10px !important;
          border: 1px solid transparent !important;
          border-radius: 12px !important;
          background: transparent !important;
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          font-weight: 620 !important;
          line-height: 1.2 !important;
          box-shadow: none !important;
          cursor: pointer !important;
          pointer-events: auto !important;
          transition: background .16s ease, color .16s ease, border-color .16s ease;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label:hover,
        [data-testid="stSidebar"] label[data-baseweb="radio"]:hover,
        [data-testid="stSidebar"] [role="radio"]:hover {
          border-color: rgba(111,159,138,.16) !important;
          background: rgba(255,255,255,.70) !important;
          color: var(--cp-text) !important;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked),
        [data-testid="stSidebar"] label[data-baseweb="radio"]:has(input:checked),
        [data-testid="stSidebar"] label:has([aria-checked="true"]),
        [data-testid="stSidebar"] [role="radio"][aria-checked="true"] {
          border-color: rgba(111,159,138,.18) !important;
          background: rgba(234,243,238,.72) !important;
          color: var(--cp-text) !important;
          box-shadow: none !important;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label p,
        [data-testid="stSidebar"] label[data-baseweb="radio"] p,
        [data-testid="stSidebar"] [data-testid="stRadio"] label span,
        [data-testid="stSidebar"] label[data-baseweb="radio"] span,
        [data-testid="stSidebar"] [role="radio"] *,
        [data-testid="stSidebar"] [role="radio"][aria-checked="true"] * {
          margin: 0 !important;
          color: inherit !important;
          line-height: 1.2 !important;
          box-shadow: none !important;
        }

        [data-testid="stSidebar"] input[type="checkbox"],
        [data-testid="stSidebar"] input[type="radio"] {
          accent-color: var(--cp-accent) !important;
        }

        [data-testid="stSidebar"] [data-testid="stTextArea"] textarea {
          min-height: 118px !important;
        }

        [data-testid="stSidebar"] [data-testid="stButton"] {
          width: auto !important;
        }

        [data-testid="stSidebar"] [data-testid="stButton"] button {
          min-height: 38px !important;
          padding: 0 13px !important;
          box-shadow: none !important;
        }

        [data-testid="stSidebar"] [data-testid="stButton"] button p {
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        [data-testid="stSidebar"] [data-testid="stButton"] button[aria-label*="删除"] {
          border-color: rgba(139,107,98,.16) !important;
          background: rgba(139,107,98,.055) !important;
          color: var(--cp-danger) !important;
        }

        [data-testid="stSidebar"] [data-testid="stButton"] button {
          min-height: 36px !important;
          border-color: rgba(111,159,138,.20) !important;
          background: rgba(255,255,255,.58) !important;
          color: var(--cp-text) !important;
          box-shadow: none !important;
        }

        [data-testid="stSidebar"] [data-testid="stButton"] button:hover {
          border-color: rgba(111,159,138,.30) !important;
          background: rgba(234,243,238,.72) !important;
        }

        /* cards and sheets */
        .cp-workspace-card,
        .cp-panel,
        .cp-result-panel,
        .cp-decision-card,
        .cp-summary-card,
        .cp-empty-state,
        .cp-revision-card,
        .cp-evidence-card,
        .cp-gap-card,
        .cp-note-card,
        .cp-job-card,
        .cp-table-toolbar,
        .cp-compact-metric,
        .cp-fact,
        .cp-overview-card,
        [data-testid="stVerticalBlockBorderWrapper"],
        [data-testid="stMetric"],
        [data-testid="stAlert"] {
          border: 1px solid rgba(29,29,31,.065) !important;
          background: var(--cp-surface) !important;
          box-shadow: var(--cp-shadow-soft) !important;
          backdrop-filter: blur(14px) saturate(1.08);
          -webkit-backdrop-filter: blur(14px) saturate(1.08);
        }

        .cp-workspace-card,
        .cp-panel,
        .cp-result-panel,
        [data-testid="stVerticalBlockBorderWrapper"] {
          border-radius: var(--cp-radius-sheet) !important;
        }

        .cp-workspace-card,
        .cp-panel,
        .cp-result-panel {
          margin: 0 0 16px;
          padding: 20px 22px;
        }

        [data-testid="stVerticalBlockBorderWrapper"] {
          padding: 0 !important;
          overflow: hidden !important;
        }

        [data-testid="stVerticalBlockBorderWrapper"] > div {
          padding: 20px 22px !important;
        }

        .cp-workspace-card-head h2,
        .cp-panel-head h3,
        .cp-section-title-block h2,
        .cp-panel-title {
          margin: 0;
          color: var(--cp-text) !important;
          font-size: clamp(20px, 1.6vw, 26px);
          font-weight: 730 !important;
          line-height: 1.24 !important;
        }

        .cp-panel-title {
          margin-bottom: 12px;
          font-size: 17px;
        }

        .cp-workspace-card-head p,
        .cp-panel-head p,
        .cp-section-title-block p,
        .cp-workspace-copy,
        .cp-mode-note {
          margin: 7px 0 0 !important;
          color: var(--cp-muted) !important;
          font-size: 14px !important;
          line-height: 1.62 !important;
        }

        .cp-section-title-block,
        .cp-workspace-head {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 18px;
          margin: 2px 0 16px;
        }

        .cp-job-card,
        .cp-revision-card,
        .cp-evidence-card,
        .cp-gap-card,
        .cp-note-card,
        .cp-compact-metric,
        .cp-fact,
        .cp-overview-card {
          border-radius: var(--cp-radius-card) !important;
        }

        .cp-job-card {
          padding: 16px !important;
          margin: 0 0 10px !important;
        }

        .cp-job-card.is-selected {
          border-color: rgba(111,159,138,.26) !important;
          background: var(--cp-accent-faint) !important;
        }

        .cp-job-card-top {
          display: grid;
          grid-template-columns: minmax(0, 1fr) auto;
          gap: 12px;
          align-items: start;
        }

        .cp-job-card-title strong,
        .cp-table-toolbar strong,
        .cp-note-card strong,
        .cp-empty-state strong,
        .cp-evidence-card-head strong,
        .cp-gap-card-head strong,
        .cp-revision-card-head strong {
          display: block;
          color: var(--cp-text) !important;
          font-weight: 690;
        }

        .cp-job-card-title strong {
          overflow: hidden;
          font-size: 15px;
          line-height: 1.35;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        .cp-job-card-title span,
        .cp-job-card-subline span,
        .cp-job-card-score-wrap em {
          color: var(--cp-muted) !important;
          font-size: 12px;
          line-height: 1.35;
        }

        .cp-job-card-title span {
          display: block;
          overflow: hidden;
          margin-top: 4px;
          font-size: 13px;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        .cp-job-card-score {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-width: 44px;
          height: 30px;
          border-radius: var(--cp-radius-pill);
          background: var(--cp-accent-faint);
          color: var(--cp-text) !important;
          font-size: 15px;
          font-weight: 720;
        }

        .cp-job-card-meta em,
        .cp-score-level,
        .cp-revision-badge,
        .cp-evidence-card-head > span,
        .cp-gap-card-head > span {
          display: inline-flex !important;
          align-items: center !important;
          gap: 5px;
          padding: 5px 9px !important;
          border: 1px solid var(--cp-line);
          border-radius: var(--cp-radius-pill) !important;
          background: rgba(255,255,255,.48) !important;
          color: var(--cp-text-2) !important;
          font-size: 11px !important;
          font-style: normal;
          font-weight: 630 !important;
        }

        /* forms */
        [data-testid="stTextInput"] input,
        [data-testid="stTextArea"] textarea,
        [data-testid="stSelectbox"] div[data-baseweb="select"] > div,
        [data-testid="stMultiSelect"] div[data-baseweb="select"] > div,
        input,
        textarea,
        select {
          min-height: 44px !important;
          border: 1px solid var(--cp-line) !important;
          border-radius: var(--cp-radius-control) !important;
          background: rgba(255,255,255,.86) !important;
          color: var(--cp-text) !important;
          font-family: var(--cp-font) !important;
          font-size: 14px !important;
          line-height: 1.5 !important;
          box-shadow: none !important;
          transition: border-color .16s ease, box-shadow .16s ease, background .16s ease;
        }

        [data-testid="stTextArea"] textarea {
          min-height: 150px !important;
          padding: 12px 14px !important;
          resize: vertical;
        }

        div[data-testid="InputInstructions"] {
          display: none !important;
          visibility: hidden !important;
          height: 0 !important;
          margin: 0 !important;
          padding: 0 !important;
        }

        .cp-auth-sheet input:invalid,
        .cp-auth-sheet input[aria-invalid="true"],
        .cp-auth-card input:invalid,
        .cp-auth-card input[aria-invalid="true"],
        .cp-auth-sheet-head ~ [data-testid="stForm"] input:invalid,
        .cp-auth-sheet-head ~ [data-testid="stForm"] input[aria-invalid="true"] {
          border-color: var(--cp-line) !important;
          box-shadow: none !important;
        }

        [data-testid="stTextInput"] input[aria-invalid="true"],
        [data-testid="stTextArea"] textarea[aria-invalid="true"] {
          border-color: var(--cp-line) !important;
          box-shadow: none !important;
        }

        [data-testid="stForm"] [data-testid="stVerticalBlock"] {
          gap: .88rem !important;
        }

        [data-testid="stFormSubmitButton"] {
          margin-top: 4px !important;
        }

        [data-testid="stMultiSelect"] div[data-baseweb="select"] > div {
          max-height: 84px !important;
          overflow-y: auto !important;
          align-content: flex-start !important;
          scrollbar-width: thin !important;
        }

        [data-testid="stMultiSelect"] [data-baseweb="tag"],
        [data-testid="stMultiSelect"] span[data-baseweb="tag"] {
          border: 1px solid rgba(111,159,138,.24) !important;
          background: rgba(234,243,238,.86) !important;
          color: var(--cp-text) !important;
          box-shadow: none !important;
        }

        [data-testid="stMultiSelect"] [data-baseweb="tag"] span,
        [data-testid="stMultiSelect"] span[data-baseweb="tag"] span {
          color: var(--cp-text) !important;
        }

        [data-testid="stMultiSelect"] [data-baseweb="tag"] svg,
        [data-testid="stMultiSelect"] span[data-baseweb="tag"] svg,
        [data-testid="stMultiSelect"] [data-baseweb="tag"] button,
        [data-testid="stMultiSelect"] span[data-baseweb="tag"] button {
          color: var(--cp-accent) !important;
          fill: var(--cp-accent) !important;
        }

        [data-testid="stTextInput"] input:focus,
        [data-testid="stTextArea"] textarea:focus,
        [data-testid="stSelectbox"] div[data-baseweb="select"] > div:focus-within,
        [data-testid="stMultiSelect"] div[data-baseweb="select"] > div:focus-within,
        input:focus,
        textarea:focus,
        select:focus {
          border-color: rgba(111,159,138,.32) !important;
          background: #fff !important;
          box-shadow: 0 0 0 3px rgba(111,159,138,.10) !important;
          outline: none !important;
        }

        [data-testid="stTextInput"] button,
        [data-testid="stTextInput"] [role="button"] {
          width: auto !important;
          min-width: 0 !important;
          min-height: 0 !important;
          height: 100% !important;
          padding: 0 12px !important;
          border: 0 !important;
          border-radius: 0 !important;
          background: transparent !important;
          box-shadow: none !important;
          color: var(--cp-muted) !important;
          transform: none !important;
        }

        [data-testid="stTextInput"] label,
        [data-testid="stTextArea"] label,
        [data-testid="stSelectbox"] label,
        [data-testid="stMultiSelect"] label,
        [data-testid="stFileUploader"] label {
          color: var(--cp-text-2) !important;
          font-size: 13px !important;
          font-weight: 620 !important;
        }

        [data-testid="stForm"] {
          border: 0 !important;
          border-radius: 0 !important;
          padding: 0 !important;
          background: transparent !important;
          box-shadow: none !important;
        }

        [data-baseweb="popover"],
        [data-baseweb="menu"] {
          border-radius: var(--cp-radius-card) !important;
          box-shadow: var(--cp-shadow-sheet) !important;
        }

        /* buttons */
        [data-testid="stButton"] button,
        [data-testid="stFormSubmitButton"] button,
        [data-testid="stDownloadButton"] button {
          min-height: 44px !important;
          border: 1px solid var(--cp-line) !important;
          border-radius: var(--cp-radius-pill) !important;
          padding: 0 18px !important;
          background: var(--cp-control-bg) !important;
          color: var(--cp-text) !important;
          font-family: var(--cp-font) !important;
          font-size: 14px !important;
          font-weight: 650 !important;
          box-shadow: 0 4px 14px rgba(0,0,0,.025) !important;
          transition: transform .12s ease, background .16s ease, border-color .16s ease, color .16s ease;
        }

        [data-testid="stButton"] button:hover,
        [data-testid="stFormSubmitButton"] button:hover,
        [data-testid="stDownloadButton"] button:hover {
          border-color: rgba(29,29,31,.12) !important;
          background: var(--cp-control-hover) !important;
          color: var(--cp-ink) !important;
        }

        [data-testid="stButton"] button:active,
        [data-testid="stFormSubmitButton"] button:active,
        [data-testid="stDownloadButton"] button:active {
          transform: translateY(1px);
        }

        [data-testid="stButton"] button[kind="primary"]:not(:disabled),
        [data-testid="stFormSubmitButton"] button[kind="primary"]:not(:disabled),
        [data-testid="stFormSubmitButton"] button[kind="primaryFormSubmit"]:not(:disabled),
        [data-testid="stBaseButton-primaryFormSubmit"]:not(:disabled),
        [data-testid="baseButton-primary"]:not(:disabled) {
          border-color: rgba(111,159,138,.24) !important;
          background: linear-gradient(180deg, rgba(243,248,245,.98), rgba(234,243,238,.96)) !important;
          color: var(--cp-text) !important;
          box-shadow: 0 5px 16px rgba(111,159,138,.10) !important;
        }

        [data-testid="stButton"] button[kind="primary"]:not(:disabled):hover,
        [data-testid="stFormSubmitButton"] button[kind="primary"]:not(:disabled):hover,
        [data-testid="stFormSubmitButton"] button[kind="primaryFormSubmit"]:not(:disabled):hover,
        [data-testid="stBaseButton-primaryFormSubmit"]:not(:disabled):hover,
        [data-testid="baseButton-primary"]:not(:disabled):hover {
          border-color: rgba(111,159,138,.34) !important;
          background: linear-gradient(180deg, rgba(255,255,255,.98), rgba(234,243,238,.98)) !important;
          color: var(--cp-text) !important;
          box-shadow: 0 7px 18px rgba(111,159,138,.12) !important;
        }

        [data-testid="stButton"] button:disabled,
        [data-testid="stFormSubmitButton"] button:disabled,
        [data-testid="stDownloadButton"] button:disabled {
          border-color: rgba(29,29,31,.06) !important;
          background: var(--cp-disabled-bg) !important;
          color: var(--cp-disabled-text) !important;
          box-shadow: none !important;
          transform: none !important;
        }

        [data-testid="stAppDeployButton"] button {
          min-height: 36px !important;
          border-radius: 999px !important;
          background: rgba(255,255,255,.72) !important;
          color: var(--cp-text) !important;
          box-shadow: 0 4px 14px rgba(0,0,0,.04) !important;
        }

        [data-testid="stTextInput"] button,
        [data-testid="stTextInput"] [role="button"] {
          width: auto !important;
          min-width: 0 !important;
          min-height: 0 !important;
          height: 100% !important;
          padding: 0 12px !important;
          border: 0 !important;
          border-radius: 0 !important;
          background: transparent !important;
          box-shadow: none !important;
          color: var(--cp-muted) !important;
          transform: none !important;
        }

        .cp-interview-action [data-testid="stButton"],
        .cp-interview-action [data-testid="stButton"] button {
          width: auto !important;
          min-width: 150px !important;
          max-width: 220px !important;
        }

        /* segmented controls */
        [data-testid="stMain"] [data-testid="stRadio"],
        .cp-auth-sheet [data-testid="stRadio"],
        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) [data-testid="stRadio"] {
          display: inline-flex;
          width: auto;
          max-width: 100%;
          margin: 0 0 16px !important;
          padding: 4px !important;
          border: 1px solid var(--cp-line) !important;
          border-radius: var(--cp-radius-pill) !important;
          background: rgba(246,244,239,.86) !important;
          box-shadow: inset 0 1px 2px rgba(0,0,0,.025) !important;
        }

        [data-testid="stMain"] [data-testid="stRadio"] div[role="radiogroup"],
        .cp-auth-sheet [data-testid="stRadio"] div[role="radiogroup"],
        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) [data-testid="stRadio"] div[role="radiogroup"] {
          display: flex !important;
          flex-wrap: wrap !important;
          gap: 4px !important;
        }

        [data-baseweb="radio"] > div:first-child,
        [data-testid="stRadio"] svg {
          display: none !important;
        }

        [data-testid="stRadio"] input[type="radio"] {
          position: absolute !important;
          inset: 0 !important;
          width: 100% !important;
          height: 100% !important;
          margin: 0 !important;
          opacity: 0 !important;
          cursor: pointer !important;
        }

        [data-testid="stMain"] [data-testid="stRadio"] label,
        [data-testid="stMain"] label[data-baseweb="radio"],
        .cp-auth-sheet [data-testid="stRadio"] label,
        .cp-auth-sheet label[data-baseweb="radio"],
        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) [data-testid="stRadio"] label,
        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) label[data-baseweb="radio"] {
          position: relative !important;
          min-height: 34px !important;
          margin: 0 !important;
          padding: 0 16px !important;
          border: 1px solid transparent !important;
          border-radius: var(--cp-radius-pill) !important;
          background: transparent !important;
          color: var(--cp-muted) !important;
          font-size: 13px !important;
          font-weight: 650 !important;
          line-height: 1.2 !important;
          cursor: pointer !important;
          pointer-events: auto !important;
          transition: background .16s ease, color .16s ease, box-shadow .16s ease;
        }

        [data-testid="stMain"] [data-testid="stRadio"] label:has(input:checked),
        [data-testid="stMain"] label[data-baseweb="radio"]:has(input:checked),
        .cp-auth-sheet [data-testid="stRadio"] label:has(input:checked),
        .cp-auth-sheet label[data-baseweb="radio"]:has(input:checked),
        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) [data-testid="stRadio"] label:has(input:checked),
        div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) label[data-baseweb="radio"]:has(input:checked) {
          background: var(--cp-surface-solid) !important;
          color: var(--cp-text) !important;
          box-shadow: 0 4px 12px rgba(0,0,0,.035) !important;
        }

        [data-testid="stMain"] [data-baseweb="tab-list"] {
          gap: 4px !important;
          width: fit-content !important;
          max-width: 100%;
          padding: 4px !important;
          border: 1px solid var(--cp-line) !important;
          border-radius: var(--cp-radius-pill) !important;
          background: rgba(246,244,239,.86) !important;
        }

        [data-baseweb="tab"] {
          min-height: 34px !important;
          border-radius: var(--cp-radius-pill) !important;
          color: var(--cp-muted) !important;
          font-family: var(--cp-font) !important;
          font-size: 13px !important;
          font-weight: 650 !important;
        }

        [data-baseweb="tab"][aria-selected="true"] {
          background: #fff !important;
          color: var(--cp-text) !important;
          box-shadow: 0 4px 12px rgba(0,0,0,.035) !important;
        }

        [data-baseweb="tab-highlight"] {
          display: none !important;
        }

        /* uploader */
        [data-testid="stFileUploader"] {
          border: 0 !important;
          background: transparent !important;
        }

        [data-testid="stFileUploader"] section,
        [data-testid="stFileUploaderDropzone"] {
          border: 1px dashed rgba(29,29,31,.12) !important;
          border-radius: var(--cp-radius-card) !important;
          background: rgba(255,255,255,.55) !important;
          box-shadow: none !important;
        }

        [data-testid="stFileUploader"] section:hover,
        [data-testid="stFileUploaderDropzone"]:hover {
          border-color: rgba(111,159,138,.24) !important;
          background: rgba(111,159,138,.045) !important;
        }

        [data-testid="stFileUploader"] small,
        [data-testid="stFileUploader"] span {
          color: var(--cp-muted) !important;
        }

        [data-testid="stExpander"] {
          border: 1px solid var(--cp-line) !important;
          border-radius: var(--cp-radius-card) !important;
          background: rgba(255,255,255,.50) !important;
          box-shadow: none !important;
          overflow: hidden !important;
        }

        [data-testid="stExpander"] summary {
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          font-weight: 640 !important;
        }

        /* tables */
        [data-testid="stDataFrame"],
        [data-testid="stTable"] {
          border: 1px solid rgba(29,29,31,.055) !important;
          border-radius: var(--cp-radius-card) !important;
          overflow: hidden !important;
          background: rgba(255,255,255,.82) !important;
          box-shadow: 0 3px 10px rgba(0,0,0,.018) !important;
        }

        [data-testid="stDataFrame"] * {
          font-family: var(--cp-font) !important;
          font-size: 13px !important;
        }

        [data-testid="stDataFrame"] [role="columnheader"],
        [data-testid="stTable"] th {
          font-size: 13px !important;
          font-weight: 650 !important;
          color: var(--cp-muted) !important;
        }

        [data-testid="stDataFrame"] [role="row"],
        [data-testid="stTable"] tr {
          min-height: 34px !important;
        }

        .cp-table-toolbar {
          display: flex;
          align-items: center;
          justify-content: space-between;
          flex-wrap: wrap;
          gap: 10px;
          margin: 0 0 12px;
          padding: 10px 12px;
          box-shadow: none !important;
        }

        /* empty states */
        .cp-empty-state {
          display: grid;
          grid-template-columns: auto minmax(0, 1fr);
          gap: 12px;
          align-items: start;
          padding: 16px 18px !important;
          margin: 4px 0 !important;
          color: var(--cp-muted) !important;
        }

        .cp-empty-state:not(.cp-empty-has-icon) {
          grid-template-columns: 1fr;
        }

        .cp-empty-state-icon {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 28px;
          height: 28px;
          border-radius: var(--cp-radius-pill);
          background: rgba(111,159,138,.10);
          color: var(--cp-accent) !important;
          font-size: 13px;
          flex: 0 0 auto;
        }

        .cp-empty-state strong {
          font-size: 15px;
          line-height: 1.35;
        }

        .cp-empty-state p {
          grid-column: 2;
          margin: 4px 0 0 !important;
          color: var(--cp-muted) !important;
          font-size: 14px !important;
          line-height: 1.55 !important;
        }

        .cp-empty-state:not(.cp-empty-has-icon) p {
          grid-column: 1;
        }

        /* charts */
        .js-plotly-plot,
        [data-testid="stPlotlyChart"] {
          border-radius: var(--cp-radius-card) !important;
          background: transparent !important;
        }

        .cp-summary-card {
          display: grid;
          gap: 14px;
          padding: 18px 20px !important;
          margin: 0 0 14px !important;
          border-radius: var(--cp-radius-card) !important;
        }

        .cp-summary-head span {
          display: block;
          color: var(--cp-muted) !important;
          font-size: 12px !important;
          font-weight: 660 !important;
          line-height: 1.35 !important;
        }

        .cp-summary-head strong {
          display: block;
          margin-top: 5px;
          color: var(--cp-text) !important;
          font-size: clamp(22px, 2.2vw, 30px);
          font-weight: 740 !important;
          line-height: 1.15 !important;
        }

        .cp-summary-copy {
          max-width: 720px;
          margin: 8px 0 0 !important;
          color: var(--cp-muted) !important;
          font-size: 14px !important;
          line-height: 1.62 !important;
        }

        .cp-summary-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(128px, 1fr));
          gap: 10px;
        }

        .cp-summary-metric {
          min-height: 74px;
          padding: 12px 13px;
          border: 1px solid rgba(29,29,31,.055);
          border-radius: 18px;
          background: rgba(255,255,255,.50);
        }

        .cp-summary-metric span {
          display: block;
          color: var(--cp-muted) !important;
          font-size: 11px !important;
          font-weight: 620 !important;
        }

        .cp-summary-metric strong {
          display: block;
          margin-top: 6px;
          color: var(--cp-text) !important;
          font-size: 16px !important;
          font-weight: 700 !important;
          line-height: 1.22 !important;
        }

        .cp-summary-actions {
          display: grid;
          gap: 8px;
        }

        .cp-summary-action {
          display: grid;
          grid-template-columns: 24px minmax(0, 1fr);
          gap: 9px;
          align-items: start;
          padding: 9px 0 0;
          border-top: 1px solid rgba(29,29,31,.055);
        }

        .cp-summary-action span {
          display: inline-grid;
          place-items: center;
          width: 22px;
          height: 22px;
          border-radius: 999px;
          background: rgba(111,159,138,.10);
          color: var(--cp-accent) !important;
          font-size: 11px;
          font-weight: 700;
        }

        .cp-summary-action p {
          margin: 1px 0 0 !important;
          color: var(--cp-text-2) !important;
          font-size: 13px !important;
          line-height: 1.55 !important;
        }

        .cp-summary-success {
          border-color: rgba(111,159,138,.16) !important;
          background: rgba(255,255,255,.76) !important;
        }

        .cp-summary-warning {
          border-color: rgba(143,118,90,.14) !important;
          background: rgba(255,255,255,.74) !important;
        }

        .cp-summary-danger {
          border-color: rgba(139,107,98,.14) !important;
          background: rgba(255,255,255,.74) !important;
        }

        /* settings center */
        .cp-settings-page {
          display: grid;
          gap: 10px;
          max-width: 1040px;
          margin: 0 auto;
        }

        .cp-settings-hero {
          padding: 14px 18px;
          border: 1px solid rgba(29,29,31,.06);
          border-radius: 20px;
          background: rgba(255,255,255,.78);
          box-shadow: var(--cp-shadow-soft);
        }

        .cp-settings-hero span {
          display: block;
          color: var(--cp-muted) !important;
          font-size: 12px !important;
          font-weight: 650 !important;
          line-height: 1.3 !important;
        }

        .cp-settings-hero h2 {
          margin: 5px 0 0 !important;
          color: var(--cp-text) !important;
          font-size: clamp(24px, 2.4vw, 30px) !important;
          font-weight: 740 !important;
          line-height: 1.15 !important;
        }

        .cp-settings-hero p {
          max-width: 780px;
          margin: 6px 0 0 !important;
          color: var(--cp-muted) !important;
          font-size: 14px !important;
          line-height: 1.45 !important;
        }

        .cp-settings-summary-grid,
        .cp-status-tile-grid {
          display: grid;
          grid-template-columns: repeat(4, minmax(0, 1fr));
          gap: 8px;
          margin: 0 0 2px;
        }

        .cp-settings-summary-card,
        .cp-settings-card,
        .cp-status-tile {
          border: 1px solid rgba(29,29,31,.06);
          border-radius: 18px;
          background: rgba(255,255,255,.78);
          box-shadow: var(--cp-shadow-soft);
        }

        .cp-settings-summary-card,
        .cp-status-tile {
          min-height: 70px;
          padding: 11px 13px;
        }

        .cp-settings-summary-card span,
        .cp-settings-card-head span,
        .cp-status-tile span {
          display: block;
          color: var(--cp-muted) !important;
          font-size: 11px !important;
          font-weight: 650 !important;
          line-height: 1.35 !important;
        }

        .cp-settings-summary-card strong,
        .cp-status-tile strong {
          display: block;
          overflow: hidden;
          margin-top: 6px;
          color: var(--cp-text) !important;
          font-size: 15px !important;
          font-weight: 700 !important;
          line-height: 1.35 !important;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        .cp-status-tile em {
          display: block;
          overflow: hidden;
          margin-top: 4px;
          color: var(--cp-muted) !important;
          font-size: 11px !important;
          font-style: normal;
          line-height: 1.35 !important;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        .cp-settings-card {
          padding: 16px 18px;
        }

        .cp-settings-card-head {
          margin: 0 0 12px;
        }

        .cp-settings-card-head strong {
          display: block;
          margin-top: 5px;
          color: var(--cp-text) !important;
          font-size: 17px !important;
          font-weight: 720 !important;
          line-height: 1.28 !important;
        }

        .cp-settings-page [data-testid="stVerticalBlockBorderWrapper"] {
          border-color: rgba(29,29,31,.06) !important;
          border-radius: 20px !important;
          background: rgba(255,255,255,.78) !important;
          box-shadow: var(--cp-shadow-soft) !important;
        }

        .cp-settings-page [data-testid="stVerticalBlockBorderWrapper"] > div {
          padding: 16px 18px !important;
        }

        .cp-settings-form-grid {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 14px;
        }

        .cp-settings-page [data-testid="stMultiSelect"],
        .cp-settings-page [data-testid="stTextInput"],
        .cp-settings-page [data-testid="stTextArea"],
        .cp-settings-page [data-testid="stNumberInput"],
        .cp-settings-page [data-testid="stSelectbox"] {
          margin-bottom: 10px !important;
        }

        .cp-settings-page [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stTextArea"] textarea {
          min-height: 96px !important;
        }

        .cp-settings-page [data-testid="stMultiSelect"] label,
        .cp-settings-page [data-testid="stTextInput"] label,
        .cp-settings-page [data-testid="stTextArea"] label,
        .cp-settings-page [data-testid="stNumberInput"] label,
        .cp-settings-page [data-testid="stSelectbox"] label {
          color: var(--cp-text-2) !important;
          font-size: 12px !important;
          font-weight: 660 !important;
        }

        .cp-settings-actions {
          margin: 2px 0 0;
          padding: 10px 0 0;
          border-top: 1px solid rgba(29,29,31,.055);
        }

        .cp-preference-compact-panel {
          display: contents;
        }

        .cp-settings-page [data-testid="stVerticalBlock"]:has(.cp-preference-compact-panel) {
          padding: 14px 16px;
          border: 1px solid rgba(29,29,31,.06);
          border-radius: 18px;
          background: rgba(255,255,255,.72);
          box-shadow: none;
        }

        .cp-preference-compact-head {
          margin-bottom: 8px !important;
        }

        .cp-settings-page [data-testid="stVerticalBlock"]:has(.cp-preference-compact-panel) [data-testid="stTextArea"] textarea {
          min-height: 64px !important;
          max-height: 72px !important;
        }

        .cp-settings-page [data-testid="stVerticalBlock"]:has(.cp-preference-compact-panel) [data-testid="stExpander"] {
          border: 0 !important;
          box-shadow: none !important;
          background: transparent !important;
        }

        .cp-settings-page [data-testid="stVerticalBlock"]:has(.cp-preference-compact-panel) [data-testid="stButton"] {
          width: auto !important;
        }

        /* metrics, scores, alerts */
        [data-testid="stMetric"],
        [data-testid="stAlert"],
        .cp-score-card,
        .cp-score-pill,
        .cp-decision-card {
          border-radius: var(--cp-radius-card) !important;
        }

        [data-testid="stAlert"] {
          border-color: rgba(29,29,31,.065) !important;
          background: rgba(255,255,255,.68) !important;
          color: var(--cp-text-2) !important;
          box-shadow: var(--cp-shadow-soft) !important;
        }

        [data-testid="stAlert"] svg {
          color: var(--cp-muted) !important;
        }

        .cp-score-grid,
        .cp-fact-grid,
        .cp-overview-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
          gap: 12px;
          margin-top: 14px;
        }

        .cp-score-card,
        .cp-score-pill {
          border: 1px solid var(--cp-line);
          background: rgba(255,255,255,.62);
          box-shadow: 0 4px 14px rgba(0,0,0,.025);
        }

        .cp-score-card-inner,
        .cp-score-pill,
        .cp-decision-card,
        .cp-compact-metric,
        .cp-fact,
        .cp-overview-card,
        .cp-note-card {
          padding: 14px 16px !important;
        }

        .cp-score-label,
        .cp-compact-metric span,
        .cp-fact-label,
        .cp-overview-label {
          display: block;
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-weight: 620;
        }

        .cp-score-num,
        .cp-compact-metric strong,
        .cp-fact-value,
        .cp-overview-value,
        .cp-decision-card-value,
        .cp-decision-value {
          display: block;
          margin-top: 5px;
          color: var(--cp-text) !important;
          font-size: 20px;
          font-weight: 720;
          line-height: 1.18;
        }

        .cp-decision-card-label,
        .cp-decision-label {
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-weight: 650;
        }

        .cp-decision-card p,
        .cp-note-card span,
        .cp-overview-copy {
          margin: 7px 0 0 !important;
          color: var(--cp-muted) !important;
          font-size: 13px !important;
          line-height: 1.58 !important;
        }

        .cp-note-card-warning,
        .cp-risk-tag-warning,
        .cp-revision-tone-gap {
          background: var(--cp-warning-soft) !important;
          border-color: rgba(143,118,90,.16) !important;
        }

        .cp-risk-tag-danger,
        .cp-job-card-low .cp-job-card-score {
          background: var(--cp-danger-soft) !important;
          color: var(--cp-danger) !important;
        }

        .cp-risk-tag-success,
        .cp-score-high .cp-job-card-score,
        .cp-score-high {
          background: var(--cp-accent-faint) !important;
        }

        .cp-revision-card,
        .cp-evidence-card,
        .cp-gap-card {
          margin: 0 0 12px !important;
          overflow: hidden !important;
        }

        .cp-revision-card-head,
        .cp-evidence-card-head,
        .cp-gap-card-head {
          display: flex !important;
          align-items: center !important;
          gap: 10px !important;
          padding: 14px 16px !important;
          border-bottom: 1px solid var(--cp-line) !important;
          background: rgba(246,244,239,.48) !important;
        }

        .cp-revision-compare {
          display: grid !important;
          grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr) !important;
          align-items: stretch !important;
        }

        .cp-revision-before,
        .cp-revision-after,
        .cp-evidence-quote,
        .cp-gap-card > p {
          padding: 16px !important;
          margin: 0 !important;
        }

        .cp-revision-before {
          border-right: 1px solid var(--cp-line) !important;
        }

        .cp-revision-after {
          background: var(--cp-accent-faint) !important;
        }

        .cp-revision-arrow {
          display: flex !important;
          align-items: center !important;
          justify-content: center !important;
          padding: 0 9px !important;
          color: var(--cp-muted) !important;
          background: rgba(246,244,239,.44) !important;
        }

        .cp-revision-label,
        .cp-revision-reason span,
        .cp-evidence-explanation span,
        .cp-gap-card-suggestion span {
          display: block;
          margin-bottom: 6px;
          color: var(--cp-muted) !important;
          font-size: 11px !important;
          font-weight: 660 !important;
        }

        .cp-revision-before p,
        .cp-revision-after p,
        .cp-gap-card > p,
        .cp-evidence-explanation p,
        .cp-gap-card-suggestion p {
          margin: 0 !important;
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          line-height: 1.62 !important;
        }

        .cp-revision-reason,
        .cp-evidence-explanation,
        .cp-gap-card-suggestion {
          padding: 12px 16px !important;
          border-top: 1px solid var(--cp-line) !important;
          background: rgba(246,244,239,.42) !important;
        }

        .cp-evidence-quote {
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          line-height: 1.62 !important;
          border: 0 !important;
          background: transparent !important;
        }

        .cp-action-list {
          display: grid;
          gap: 8px;
          margin: 8px 0 0;
        }

        .cp-action-row {
          display: grid;
          grid-template-columns: 26px minmax(0, 1fr);
          gap: 10px;
          align-items: start;
          padding: 10px 0;
          border-bottom: 1px solid var(--cp-line);
        }

        .cp-action-row:last-child {
          border-bottom: 0;
        }

        .cp-action-row span {
          display: inline-grid;
          place-items: center;
          width: 24px;
          height: 24px;
          border-radius: var(--cp-radius-pill);
          background: var(--cp-accent-faint);
          color: var(--cp-accent) !important;
          font-size: 12px;
          font-weight: 720;
        }

        .cp-action-row p {
          margin: 1px 0 0 !important;
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          line-height: 1.55 !important;
        }

        /* responsive */
        @media (max-width: 900px) {
          .block-container {
            padding: 1.25rem 1rem 3rem !important;
          }

          section[data-testid="stSidebar"],
          [data-testid="stSidebar"] {
            width: var(--cp-sidebar-width) !important;
            min-width: var(--cp-sidebar-width) !important;
            padding: 8px !important;
          }

          [data-testid="stSidebarContent"] {
            width: calc(var(--cp-sidebar-width) - 16px) !important;
            min-width: calc(var(--cp-sidebar-width) - 16px) !important;
            height: calc(100vh - 16px) !important;
            max-height: calc(100vh - 16px) !important;
          }

          .cp-topbar,
          .cp-section-title-block,
          .cp-workspace-head {
            display: grid;
            grid-template-columns: 1fr;
          }

          .cp-topbar-chips {
            justify-content: flex-start;
          }

          div[data-testid="stMainBlockContainer"]:has(.cp-auth-scope) {
            padding-top: 32px !important;
            padding-bottom: 32px !important;
          }

          .cp-auth-story h1 {
            font-size: 38px;
          }

          .cp-settings-summary-grid,
          .cp-status-tile-grid,
          .cp-settings-form-grid {
            grid-template-columns: 1fr;
          }

          .cp-auth-sheet,
          .cp-auth-card,
          .cp-workspace-card,
          .cp-panel,
          .cp-result-panel,
          [data-testid="stVerticalBlockBorderWrapper"] > div {
            padding: 20px !important;
          }

          .cp-revision-compare {
            grid-template-columns: 1fr !important;
          }

          .cp-revision-before {
            border-right: 0 !important;
            border-bottom: 1px solid var(--cp-line) !important;
          }

          .cp-revision-arrow {
            min-height: 32px;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
