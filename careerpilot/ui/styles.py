from __future__ import annotations

import streamlit as st


def inject_global_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
          --cp-bg: #F5F5F7;
          --cp-surface: #FFFFFF;
          --cp-surface-subtle: #FAFAFB;
          --cp-surface-muted: #F0F2F4;
          --cp-text: #111827;
          --cp-text-2: #374151;
          --cp-muted: #6B7280;
          --cp-muted-2: #9CA3AF;
          --cp-brand: #0E6B5A;
          --cp-brand-2: #0F766E;
          --cp-brand-dark: #0B4F43;
          --cp-brand-soft: #E7F3F0;
          --cp-brand-soft-2: #F2F8F6;
          --cp-danger: #DC2626;
          --cp-danger-soft: #FEF2F2;
          --cp-warning: #B45309;
          --cp-warning-soft: #FFF7ED;
          --cp-info: #2563EB;
          --cp-info-soft: #EFF6FF;
          --cp-line: #E5E7EB;
          --cp-line-strong: #D1D5DB;
          --cp-shadow: 0 12px 30px rgba(15, 23, 42, 0.06);
          --cp-shadow-soft: 0 8px 22px rgba(15, 23, 42, 0.045);
          --cp-radius-xl: 24px;
          --cp-radius-lg: 18px;
          --cp-radius-md: 14px;
          --cp-radius-pill: 999px;
          --cp-sidebar-width: 300px;
          --cp-font: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "PingFang SC", "Microsoft YaHei", sans-serif;
        }

        * {
          box-sizing: border-box;
          letter-spacing: 0 !important;
        }

        html,
        body,
        .stApp {
          font-family: var(--cp-font) !important;
          color: var(--cp-text) !important;
          background: var(--cp-bg) !important;
        }

        #MainMenu,
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

        .stApp {
          min-height: 100vh;
          background: var(--cp-bg) !important;
        }

        [data-testid="stAppViewContainer"],
        section[data-testid="stMain"],
        div[data-testid="stMainBlockContainer"] {
          background: transparent !important;
        }

        .block-container {
          max-width: 1220px;
          padding: 2rem 2.25rem 4rem !important;
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
          font-weight: 760 !important;
        }

        [data-testid="stMarkdownContainer"] p,
        [data-testid="stMarkdownContainer"] li,
        label,
        p {
          color: var(--cp-text-2) !important;
          font-size: 15px;
          line-height: 1.62;
          font-weight: 450;
        }

        small,
        [data-testid="stCaptionContainer"],
        [data-testid="stMarkdownContainer"] small {
          color: var(--cp-muted) !important;
          font-size: 13px !important;
          line-height: 1.5 !important;
        }

        section[data-testid="stSidebar"],
        [data-testid="stSidebar"] {
          width: var(--cp-sidebar-width) !important;
          min-width: var(--cp-sidebar-width) !important;
          padding: 12px 0 12px 12px !important;
          border-right: 1px solid rgba(209, 213, 219, 0.65) !important;
          background: rgba(245, 245, 247, 0.86) !important;
          box-shadow: none !important;
        }

        section[data-testid="stSidebar"] > div:first-child,
        [data-testid="stSidebar"] > div:first-child {
          width: var(--cp-sidebar-width) !important;
          min-width: var(--cp-sidebar-width) !important;
          background: transparent !important;
          box-shadow: none !important;
          border: 0 !important;
        }

        [data-testid="stSidebarContent"] {
          width: calc(var(--cp-sidebar-width) - 12px) !important;
          min-width: calc(var(--cp-sidebar-width) - 12px) !important;
          height: calc(100vh - 24px) !important;
          max-height: calc(100vh - 24px) !important;
          margin: 0 !important;
          padding: 16px 14px 22px !important;
          border: 1px solid rgba(229, 231, 235, 0.9) !important;
          border-radius: 24px !important;
          background: rgba(250, 250, 251, 0.86) !important;
          box-shadow: none !important;
          overflow-y: auto !important;
          overflow-x: hidden !important;
          overscroll-behavior: contain !important;
          scrollbar-width: thin !important;
          scrollbar-color: rgba(107, 114, 128, 0.35) transparent !important;
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar {
          width: 6px;
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar-track {
          background: transparent;
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar-thumb {
          border-radius: var(--cp-radius-pill);
          background: rgba(107, 114, 128, 0.28);
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
          gap: 0.75rem !important;
        }

        .cp-sidebar-brand,
        .cp-sidebar-status-card,
        .cp-sidebar-status {
          border: 1px solid var(--cp-line) !important;
          border-radius: var(--cp-radius-lg) !important;
          background: var(--cp-surface) !important;
          box-shadow: none !important;
        }

        .cp-sidebar-brand {
          padding: 16px !important;
          margin-bottom: 10px !important;
        }

        .cp-sidebar-brand-row {
          display: flex;
          align-items: center;
          gap: 12px;
        }

        .cp-logo-mark {
          display: grid;
          place-items: center;
          width: 44px;
          height: 44px;
          flex: 0 0 44px;
          border-radius: 14px;
          background: var(--cp-brand);
          color: #fff;
          font-size: 14px;
          font-weight: 780;
          box-shadow: 0 10px 22px rgba(14, 107, 90, 0.18);
        }

        .cp-sidebar-brand strong,
        .cp-sidebar-title {
          display: block;
          color: var(--cp-text) !important;
          font-size: 18px;
          line-height: 1.18;
          font-weight: 760;
        }

        .cp-sidebar-brand em,
        .cp-sidebar-meta,
        .cp-sidebar-kicker,
        .cp-sidebar-copy {
          display: block;
          margin-top: 4px;
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-style: normal;
          line-height: 1.45;
          font-weight: 560;
        }

        .cp-sidebar-kicker {
          color: var(--cp-brand) !important;
          font-weight: 700;
        }

        .cp-sidebar-status-card,
        .cp-sidebar-status {
          padding: 14px !important;
          margin-bottom: 12px !important;
        }

        .cp-sidebar-status-row {
          display: grid;
          grid-template-columns: 72px minmax(0, 1fr);
          gap: 10px !important;
          align-items: center;
          min-height: 30px !important;
        }

        .cp-sidebar-status-row span {
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-weight: 620;
        }

        .cp-sidebar-status-row strong {
          overflow: hidden;
          color: var(--cp-text) !important;
          font-size: 13px;
          font-weight: 680;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        .cp-sidebar-block-title,
        .cp-sidebar-toolbox-title {
          margin: 14px 8px 6px;
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-weight: 700;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] {
          padding: 0 !important;
          border: 0 !important;
          background: transparent !important;
        }

        [data-testid="stSidebar"] [role="radiogroup"] {
          display: grid !important;
          gap: 6px !important;
          background: transparent !important;
        }

        [data-testid="stSidebar"] [role="radiogroup"] input[type="radio"],
        [data-testid="stSidebar"] [role="radiogroup"] svg,
        [data-testid="stSidebar"] [data-baseweb="radio"] > div:first-child {
          display: none !important;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label,
        [data-testid="stSidebar"] label[data-baseweb="radio"] {
          display: flex !important;
          align-items: center !important;
          min-height: 42px !important;
          margin: 0 !important;
          padding: 0 14px !important;
          border: 1px solid transparent !important;
          border-radius: var(--cp-radius-pill) !important;
          background: transparent !important;
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          font-weight: 630 !important;
          line-height: 1.2 !important;
          transition: background .16s ease, color .16s ease, border-color .16s ease;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label:hover,
        [data-testid="stSidebar"] label[data-baseweb="radio"]:hover {
          background: var(--cp-surface-muted) !important;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked),
        [data-testid="stSidebar"] label[data-baseweb="radio"]:has(input:checked) {
          border-color: rgba(14, 107, 90, 0.16) !important;
          background: var(--cp-brand-soft) !important;
          color: var(--cp-brand-dark) !important;
          font-weight: 720 !important;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) * {
          color: var(--cp-brand-dark) !important;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label p,
        [data-testid="stSidebar"] label[data-baseweb="radio"] p,
        [data-testid="stSidebar"] [data-testid="stRadio"] label span,
        [data-testid="stSidebar"] label[data-baseweb="radio"] span {
          margin: 0 !important;
          line-height: 1.2 !important;
        }

        [data-testid="stRadio"] input[type="radio"] {
          display: none !important;
        }

        .cp-topbar {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 20px;
          min-height: 84px;
          margin: 0 0 18px;
          padding: 4px 2px 18px;
          border-bottom: 1px solid var(--cp-line);
          background: transparent !important;
        }

        .cp-topbar h1 {
          margin: 0;
          color: var(--cp-text) !important;
          font-size: clamp(28px, 2.8vw, 38px);
          line-height: 1.14;
          font-weight: 780 !important;
        }

        .cp-topbar-main {
          min-width: 0;
        }

        .cp-topbar-eyebrow {
          display: block;
          margin: 0 0 8px;
          color: var(--cp-brand) !important;
          font-size: 12px;
          font-weight: 720;
        }

        .cp-topbar p {
          margin: 8px 0 0;
          max-width: 760px;
          color: var(--cp-muted) !important;
          font-size: 15px;
          line-height: 1.58;
          font-weight: 450;
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
          padding-top: 4px;
        }

        .cp-topbar-chip {
          display: inline-flex;
          align-items: center;
          min-height: 30px;
          padding: 6px 11px;
          border: 1px solid rgba(14, 107, 90, 0.13);
          border-radius: var(--cp-radius-pill);
          background: var(--cp-brand-soft-2);
          color: var(--cp-brand-dark) !important;
          font-size: 12px;
          font-weight: 650;
          white-space: nowrap;
        }

        .cp-workspace-card,
        .cp-panel,
        .cp-result-panel,
        .cp-decision-card,
        .cp-empty-state,
        .cp-revision-card,
        .cp-evidence-card,
        .cp-gap-card,
        .cp-note-card,
        [data-testid="stVerticalBlockBorderWrapper"],
        [data-testid="stMetric"],
        [data-testid="stAlert"] {
          border: 1px solid var(--cp-line) !important;
          background: var(--cp-surface) !important;
          box-shadow: var(--cp-shadow) !important;
        }

        .cp-workspace-card,
        .cp-panel,
        .cp-result-panel,
        [data-testid="stVerticalBlockBorderWrapper"] {
          border-radius: var(--cp-radius-xl) !important;
        }

        .cp-workspace-card,
        .cp-panel,
        .cp-result-panel {
          margin: 0 0 14px;
          padding: 24px 26px;
        }

        [data-testid="stVerticalBlockBorderWrapper"] {
          padding: 0 !important;
          overflow: hidden !important;
        }

        [data-testid="stVerticalBlockBorderWrapper"] > div {
          padding: 24px 26px !important;
        }

        .cp-workspace-card-head h2,
        .cp-panel-head h3,
        .cp-section-title-block h2,
        .cp-panel-title {
          margin: 0;
          color: var(--cp-text) !important;
          font-size: clamp(20px, 1.8vw, 26px);
          line-height: 1.24;
          font-weight: 740 !important;
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
          margin: 7px 0 0;
          color: var(--cp-muted) !important;
          font-size: 14px;
          line-height: 1.58;
        }

        .cp-section-title-block {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 18px;
          margin: 2px 0 14px;
        }

        .cp-section-title-block > span,
        .cp-subsection-label,
        .cp-field-label {
          color: var(--cp-muted) !important;
          font-size: 12px !important;
          font-weight: 700 !important;
        }

        .cp-subsection-label {
          margin: 18px 0 8px;
        }

        .cp-workspace-head {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 18px;
          margin-bottom: 16px;
        }

        .cp-workspace-eyebrow {
          color: var(--cp-brand) !important;
          font-size: 12px;
          font-weight: 720;
          margin-bottom: 4px;
        }

        .cp-workspace-title {
          color: var(--cp-text) !important;
          font-size: 24px;
          font-weight: 760;
          line-height: 1.22;
        }

        [data-testid="stMain"] [data-testid="stRadio"] {
          display: inline-flex;
          width: auto;
          max-width: 100%;
          margin: 0 0 18px !important;
          padding: 4px !important;
          border: 1px solid var(--cp-line) !important;
          border-radius: var(--cp-radius-pill) !important;
          background: #EBEDF0 !important;
          box-shadow: none !important;
        }

        [data-testid="stMain"] [data-testid="stRadio"] div[role="radiogroup"] {
          display: flex !important;
          flex-wrap: wrap !important;
          gap: 4px !important;
        }

        [data-testid="stMain"] [data-testid="stRadio"] label,
        [data-testid="stMain"] label[data-baseweb="radio"] {
          min-height: 34px !important;
          margin: 0 !important;
          padding: 0 14px !important;
          border-radius: var(--cp-radius-pill) !important;
          border: 1px solid transparent !important;
          background: transparent !important;
          color: var(--cp-muted) !important;
          font-size: 13px !important;
          font-weight: 650 !important;
          transition: background .16s ease, color .16s ease, box-shadow .16s ease;
        }

        [data-testid="stMain"] [data-testid="stRadio"] label:has(input:checked),
        [data-testid="stMain"] label[data-baseweb="radio"]:has(input:checked) {
          background: var(--cp-surface) !important;
          color: var(--cp-brand-dark) !important;
          box-shadow: 0 2px 7px rgba(15, 23, 42, 0.08) !important;
        }

        [data-testid="stButton"] button,
        button {
          min-height: 38px !important;
          border-radius: var(--cp-radius-pill) !important;
          border: 1px solid var(--cp-line-strong) !important;
          background: var(--cp-surface) !important;
          color: var(--cp-text) !important;
          font-family: var(--cp-font) !important;
          font-size: 14px !important;
          font-weight: 680 !important;
          box-shadow: none !important;
          transition: background .16s ease, border-color .16s ease, color .16s ease, transform .12s ease;
        }

        [data-testid="stButton"] button:hover,
        button:hover {
          border-color: rgba(14, 107, 90, 0.26) !important;
          background: var(--cp-brand-soft-2) !important;
          color: var(--cp-brand-dark) !important;
        }

        [data-testid="stButton"] button:active,
        button:active {
          transform: translateY(1px);
        }

        [data-testid="stButton"] button[kind="primary"],
        [data-testid="baseButton-primary"] {
          border-color: var(--cp-brand) !important;
          background: var(--cp-brand) !important;
          color: #fff !important;
        }

        [data-testid="stButton"] button[kind="primary"]:hover,
        [data-testid="baseButton-primary"]:hover {
          border-color: var(--cp-brand-dark) !important;
          background: var(--cp-brand-dark) !important;
          color: #fff !important;
        }

        [data-testid="stTextInput"] input,
        [data-testid="stTextArea"] textarea,
        [data-testid="stSelectbox"] div[data-baseweb="select"] > div,
        [data-testid="stMultiSelect"] div[data-baseweb="select"] > div,
        input,
        textarea,
        select {
          border: 1px solid var(--cp-line-strong) !important;
          border-radius: 14px !important;
          background: var(--cp-surface) !important;
          color: var(--cp-text) !important;
          font-family: var(--cp-font) !important;
          font-size: 14px !important;
          box-shadow: none !important;
        }

        [data-testid="stTextInput"] input:focus,
        [data-testid="stTextArea"] textarea:focus,
        [data-testid="stSelectbox"] div[data-baseweb="select"] > div:focus-within,
        input:focus,
        textarea:focus,
        select:focus {
          border-color: rgba(14, 107, 90, 0.48) !important;
          box-shadow: 0 0 0 3px rgba(14, 107, 90, 0.10) !important;
          outline: none !important;
        }

        [data-testid="stTextInput"] label,
        [data-testid="stTextArea"] label,
        [data-testid="stSelectbox"] label,
        [data-testid="stMultiSelect"] label {
          color: var(--cp-muted) !important;
          font-size: 13px !important;
          font-weight: 650 !important;
        }

        [data-testid="stTabs"] [data-baseweb="tab-list"] {
          gap: 4px !important;
          padding: 4px !important;
          border: 1px solid var(--cp-line) !important;
          border-radius: var(--cp-radius-pill) !important;
          background: #EBEDF0 !important;
        }

        [data-testid="stTabs"] [data-baseweb="tab"] {
          height: 34px !important;
          padding: 0 16px !important;
          border-radius: var(--cp-radius-pill) !important;
          color: var(--cp-muted) !important;
          font-size: 13px !important;
          font-weight: 650 !important;
        }

        [data-testid="stTabs"] [aria-selected="true"] {
          background: var(--cp-surface) !important;
          color: var(--cp-brand-dark) !important;
          box-shadow: 0 2px 7px rgba(15, 23, 42, 0.08) !important;
        }

        [data-testid="stTabs"] [data-baseweb="tab-highlight"],
        [data-testid="stTabs"] [data-baseweb="tab-border"] {
          display: none !important;
        }

        [data-testid="stExpander"] {
          border: 1px solid var(--cp-line) !important;
          border-radius: var(--cp-radius-lg) !important;
          background: var(--cp-surface) !important;
          box-shadow: var(--cp-shadow-soft) !important;
          overflow: hidden !important;
        }

        [data-testid="stExpander"] details,
        [data-testid="stExpander"] summary {
          color: var(--cp-text) !important;
          font-weight: 650 !important;
        }

        [data-testid="stDataFrame"],
        [data-testid="stDataFrameResizable"],
        div[data-testid="stDataFrame"] > div {
          border-radius: var(--cp-radius-lg) !important;
          border-color: var(--cp-line) !important;
          background: var(--cp-surface) !important;
          box-shadow: var(--cp-shadow-soft) !important;
          overflow: hidden !important;
        }

        .cp-score-grid {
          display: grid !important;
          grid-template-columns: repeat(auto-fit, minmax(156px, 1fr)) !important;
          gap: 12px !important;
          margin: 12px 0 18px !important;
        }

        .cp-score-card {
          border: 1px solid var(--cp-line) !important;
          border-radius: var(--cp-radius-lg) !important;
          padding: 0 !important;
          background: var(--cp-surface) !important;
          box-shadow: var(--cp-shadow-soft) !important;
        }

        .cp-score-card-inner {
          padding: 18px !important;
        }

        .cp-score-label {
          display: block !important;
          margin-bottom: 10px !important;
          color: var(--cp-muted) !important;
          font-size: 12px !important;
          font-weight: 680 !important;
        }

        .cp-score-value-row {
          display: grid !important;
          grid-template-columns: auto minmax(64px, 1fr) !important;
          align-items: end !important;
          gap: 12px !important;
        }

        .cp-score-num {
          color: var(--cp-text) !important;
          font-size: 38px !important;
          font-weight: 780 !important;
          line-height: 1 !important;
        }

        .cp-score-high .cp-score-num {
          color: var(--cp-brand-dark) !important;
        }

        .cp-score-medium .cp-score-num {
          color: var(--cp-warning) !important;
        }

        .cp-score-low .cp-score-num {
          color: var(--cp-danger) !important;
        }

        .cp-score-ring {
          width: 100% !important;
          height: 6px !important;
          margin-bottom: 7px !important;
          border-radius: var(--cp-radius-pill) !important;
          background:
            linear-gradient(90deg, currentColor calc(var(--pct, 0) * 1%), #E5E7EB 0) !important;
          opacity: 1 !important;
        }

        .cp-score-high .cp-score-ring {
          color: var(--cp-brand) !important;
        }

        .cp-score-medium .cp-score-ring {
          color: #D97706 !important;
        }

        .cp-score-low .cp-score-ring {
          color: var(--cp-danger) !important;
        }

        .cp-score-level,
        .cp-score-pill {
          display: inline-flex !important;
          align-items: center !important;
          border-radius: var(--cp-radius-pill) !important;
          font-style: normal !important;
        }

        .cp-score-level {
          padding: 4px 10px !important;
          background: var(--cp-surface-muted) !important;
          color: var(--cp-muted) !important;
          font-size: 12px !important;
          font-weight: 650 !important;
        }

        .cp-score-card-foot {
          display: flex;
          align-items: center;
          flex-wrap: wrap;
          gap: 8px;
          margin-top: 10px;
        }

        .cp-score-sub {
          margin: 0 !important;
          color: var(--cp-muted) !important;
          font-size: 13px !important;
          line-height: 1.45 !important;
        }

        .cp-score-pill {
          gap: 8px;
          min-height: 34px;
          padding: 6px 12px !important;
          border: 1px solid rgba(14, 107, 90, 0.14) !important;
          background: var(--cp-brand-soft-2) !important;
          color: var(--cp-brand-dark) !important;
          box-shadow: none !important;
        }

        .cp-score-pill span,
        .cp-score-pill em {
          color: inherit !important;
          font-size: 12px !important;
          font-weight: 650 !important;
        }

        .cp-score-pill strong {
          color: inherit !important;
          font-size: 17px !important;
          font-weight: 780 !important;
        }

        .cp-risk-tag-row {
          margin: 8px 0 14px;
        }

        .cp-risk-tag {
          display: inline-flex !important;
          align-items: center !important;
          min-height: 28px !important;
          padding: 5px 10px !important;
          border-radius: var(--cp-radius-pill) !important;
          border: 1px solid var(--cp-line) !important;
          background: var(--cp-surface-muted) !important;
          color: var(--cp-muted) !important;
          font-size: 12px !important;
          font-weight: 650 !important;
          line-height: 1.2 !important;
        }

        .cp-risk-tag-success {
          border-color: rgba(14, 107, 90, 0.15) !important;
          background: var(--cp-brand-soft-2) !important;
          color: var(--cp-brand-dark) !important;
        }

        .cp-risk-tag-warning {
          border-color: rgba(180, 83, 9, 0.16) !important;
          background: var(--cp-warning-soft) !important;
          color: var(--cp-warning) !important;
        }

        .cp-risk-tag-danger {
          border-color: rgba(220, 38, 38, 0.16) !important;
          background: var(--cp-danger-soft) !important;
          color: var(--cp-danger) !important;
        }

        .cp-risk-tag-info {
          border-color: rgba(37, 99, 235, 0.14) !important;
          background: var(--cp-info-soft) !important;
          color: var(--cp-info) !important;
        }

        .cp-decision-card {
          position: relative;
          border-radius: var(--cp-radius-lg) !important;
          padding: 18px 20px !important;
          margin: 0 0 14px !important;
        }

        .cp-decision-card::before {
          content: "";
          position: absolute;
          left: 0;
          top: 18px;
          bottom: 18px;
          width: 3px;
          border-radius: var(--cp-radius-pill);
          background: var(--cp-brand);
        }

        .cp-decision-card > span,
        .cp-decision-label,
        .cp-decision-card-label {
          display: block !important;
          margin-bottom: 6px !important;
          color: var(--cp-muted) !important;
          font-size: 12px !important;
          font-weight: 700 !important;
        }

        .cp-decision-card > strong,
        .cp-decision-value,
        .cp-decision-card-value {
          display: block !important;
          color: var(--cp-text) !important;
          font-size: 22px !important;
          line-height: 1.2 !important;
          font-weight: 760 !important;
        }

        .cp-decision-card > p,
        .cp-decision-copy {
          margin: 8px 0 0 !important;
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          line-height: 1.58 !important;
        }

        .cp-job-card {
          border: 1px solid var(--cp-line) !important;
          border-radius: var(--cp-radius-lg) !important;
          padding: 16px !important;
          margin: 0 0 10px !important;
          background: var(--cp-surface) !important;
          box-shadow: var(--cp-shadow-soft) !important;
        }

        .cp-job-card.is-selected {
          border-color: rgba(14, 107, 90, 0.32) !important;
          box-shadow: 0 12px 28px rgba(14, 107, 90, 0.10) !important;
        }

        .cp-job-card-top {
          display: grid;
          grid-template-columns: minmax(0, 1fr) auto;
          gap: 12px;
          align-items: start;
        }

        .cp-job-card-title strong {
          display: block;
          overflow: hidden;
          color: var(--cp-text) !important;
          font-size: 15px;
          font-weight: 720;
          line-height: 1.35;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        .cp-job-card-title span {
          display: block;
          overflow: hidden;
          margin-top: 4px;
          color: var(--cp-muted) !important;
          font-size: 13px;
          line-height: 1.35;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        .cp-job-card-score-wrap {
          display: grid;
          justify-items: end;
          gap: 4px;
        }

        .cp-job-card-score-wrap em {
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-style: normal;
          font-weight: 650;
          white-space: nowrap;
        }

        .cp-job-card-score {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-width: 44px;
          height: 30px;
          border-radius: var(--cp-radius-pill);
          background: var(--cp-brand-soft);
          color: var(--cp-brand-dark) !important;
          font-size: 15px;
          font-weight: 760;
        }

        .cp-job-card-low .cp-job-card-score {
          background: var(--cp-danger-soft);
          color: var(--cp-danger) !important;
        }

        .cp-job-card-medium .cp-job-card-score {
          background: var(--cp-warning-soft);
          color: var(--cp-warning) !important;
        }

        .cp-job-card-subline {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 10px;
          margin-top: 8px;
        }

        .cp-job-card-subline span {
          overflow: hidden;
          color: var(--cp-muted) !important;
          font-size: 13px;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        .cp-job-card-subline strong {
          color: var(--cp-text-2) !important;
          font-size: 12px;
          font-weight: 680;
          white-space: nowrap;
        }

        .cp-job-card-meta {
          gap: 6px;
          margin-top: 12px;
        }

        .cp-job-card-meta em {
          display: inline-flex;
          align-items: center;
          gap: 5px;
          padding: 5px 8px;
          border: 1px solid var(--cp-line);
          border-radius: var(--cp-radius-pill);
          background: var(--cp-surface-subtle);
          font-style: normal;
        }

        .cp-job-card-meta span {
          color: var(--cp-muted) !important;
          font-size: 11px;
          font-weight: 620;
        }

        .cp-job-card-meta strong {
          color: var(--cp-text) !important;
          font-size: 12px;
          font-weight: 720;
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
          background: var(--cp-brand-soft);
          color: var(--cp-brand-dark) !important;
          font-size: 12px;
          font-weight: 720;
        }

        .cp-action-row p {
          margin: 1px 0 0 !important;
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          line-height: 1.55 !important;
        }

        .cp-empty-state {
          display: grid;
          grid-template-columns: auto minmax(0, 1fr);
          gap: 12px;
          align-items: start;
          border-radius: var(--cp-radius-lg) !important;
          padding: 16px 18px !important;
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
          background: var(--cp-surface-muted);
          color: var(--cp-muted) !important;
          font-size: 13px;
          flex: 0 0 auto;
        }

        .cp-empty-state strong {
          display: block;
          color: var(--cp-text) !important;
          font-size: 15px;
          font-weight: 720;
        }

        .cp-empty-state p {
          grid-column: 2;
          margin: 4px 0 0 !important;
          color: var(--cp-muted) !important;
          font-size: 14px !important;
          line-height: 1.5 !important;
        }

        .cp-empty-state:not(.cp-empty-has-icon) p {
          grid-column: 1;
        }

        .cp-revision-card,
        .cp-evidence-card,
        .cp-gap-card,
        .cp-note-card {
          border-radius: var(--cp-radius-lg) !important;
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
          background: var(--cp-surface-subtle) !important;
        }

        .cp-revision-badge,
        .cp-evidence-card-head > span,
        .cp-gap-card-head > span {
          display: inline-flex !important;
          align-items: center !important;
          padding: 4px 9px !important;
          border-radius: var(--cp-radius-pill) !important;
          background: var(--cp-brand-soft) !important;
          color: var(--cp-brand-dark) !important;
          font-size: 11px !important;
          font-weight: 700 !important;
          white-space: nowrap !important;
        }

        .cp-revision-card-head strong,
        .cp-evidence-card-head strong,
        .cp-gap-card-head strong {
          color: var(--cp-text) !important;
          font-size: 14px !important;
          font-weight: 700 !important;
          line-height: 1.35 !important;
        }

        .cp-evidence-card-head em {
          margin-left: auto;
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-style: normal;
          font-weight: 650;
        }

        .cp-revision-compare {
          display: grid !important;
          grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr) !important;
          align-items: stretch !important;
        }

        .cp-revision-before,
        .cp-revision-after {
          padding: 16px !important;
        }

        .cp-revision-before {
          border-right: 1px solid var(--cp-line) !important;
          background: var(--cp-surface) !important;
        }

        .cp-revision-after {
          background: var(--cp-brand-soft-2) !important;
        }

        .cp-revision-arrow {
          display: flex !important;
          align-items: center !important;
          justify-content: center !important;
          padding: 0 9px !important;
          color: var(--cp-muted) !important;
          background: var(--cp-surface-subtle) !important;
        }

        .cp-revision-label {
          display: flex !important;
          align-items: center !important;
          gap: 6px !important;
          margin-bottom: 8px !important;
          color: var(--cp-muted) !important;
          font-size: 11px !important;
          font-weight: 720 !important;
        }

        .cp-revision-dot {
          width: 6px !important;
          height: 6px !important;
          border-radius: 50% !important;
          display: inline-block !important;
          background: currentColor !important;
        }

        .cp-revision-label-after {
          color: var(--cp-brand-dark) !important;
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
          background: var(--cp-surface-subtle) !important;
        }

        .cp-revision-reason span,
        .cp-evidence-explanation span,
        .cp-gap-card-suggestion span {
          display: block;
          margin-bottom: 5px;
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-weight: 700;
        }

        .cp-evidence-quote {
          margin: 0 !important;
          padding: 16px !important;
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          line-height: 1.62 !important;
          border: 0 !important;
          background: var(--cp-surface) !important;
        }

        .cp-gap-card > p {
          padding: 16px !important;
        }

        .cp-revision-tone-gap .cp-gap-card-head > span {
          background: var(--cp-warning-soft) !important;
          color: var(--cp-warning) !important;
        }

        .cp-revision-tone-evidence .cp-evidence-card-head > span {
          background: var(--cp-info-soft) !important;
          color: var(--cp-info) !important;
        }

        .cp-table-toolbar {
          display: flex;
          align-items: center;
          justify-content: space-between;
          flex-wrap: wrap;
          gap: 10px;
          margin: 0 0 12px;
          padding: 12px 14px;
          border: 1px solid var(--cp-line);
          border-radius: var(--cp-radius-lg);
          background: var(--cp-surface);
          box-shadow: var(--cp-shadow-soft);
        }

        .cp-table-toolbar strong {
          color: var(--cp-text) !important;
          font-size: 15px;
          font-weight: 720;
        }

        .cp-table-count {
          display: inline-flex;
          align-items: center;
          min-height: 28px;
          padding: 4px 10px;
          border-radius: var(--cp-radius-pill);
          background: var(--cp-surface-muted);
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-weight: 650;
        }

        .cp-compact-metric,
        .cp-fact,
        .cp-overview-card {
          border: 1px solid var(--cp-line);
          border-radius: var(--cp-radius-lg);
          background: var(--cp-surface);
          box-shadow: var(--cp-shadow-soft);
          padding: 14px 16px;
        }

        .cp-compact-metric span,
        .cp-fact-label,
        .cp-overview-label {
          display: block;
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-weight: 650;
        }

        .cp-compact-metric strong,
        .cp-fact-value,
        .cp-overview-value {
          display: block;
          margin-top: 4px;
          color: var(--cp-text) !important;
          font-size: 20px;
          font-weight: 760;
          line-height: 1.2;
        }

        .cp-fact-grid,
        .cp-overview-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
          gap: 12px;
          margin-top: 14px;
        }

        .cp-overview-copy {
          margin-top: 6px;
          color: var(--cp-muted) !important;
          font-size: 13px;
          line-height: 1.5;
        }

        .cp-note-card {
          display: grid;
          gap: 4px;
          padding: 14px 16px !important;
        }

        .cp-note-card strong {
          color: var(--cp-text) !important;
          font-size: 14px;
          font-weight: 720;
        }

        .cp-note-card span {
          color: var(--cp-muted) !important;
          font-size: 13px;
          line-height: 1.5;
        }

        .cp-note-card-warning {
          border-color: rgba(180, 83, 9, 0.16) !important;
          background: var(--cp-warning-soft) !important;
        }

        .cp-auth-hero {
          min-height: calc(100vh - 48px);
          display: grid;
          grid-template-columns: minmax(0, 1.05fr) minmax(360px, 0.75fr);
          gap: 38px;
          align-items: center;
          padding: 42px 0;
        }

        .cp-auth-showcase {
          min-height: calc(100vh - 72px);
          display: grid;
          align-items: center;
          padding: 42px 0;
        }

        .cp-auth-showcase-inner {
          display: grid;
          grid-template-columns: minmax(0, 1fr) minmax(360px, 430px);
          gap: 42px;
          align-items: center;
        }

        .cp-auth-card {
          border-radius: var(--cp-radius-xl) !important;
          padding: 28px !important;
          background: var(--cp-surface) !important;
          border: 1px solid var(--cp-line) !important;
          box-shadow: var(--cp-shadow) !important;
        }

        .cp-auth-kicker,
        .cp-auth-showcase-kicker,
        .cp-auth-card-kicker {
          color: var(--cp-brand) !important;
          font-size: 13px;
          font-weight: 740;
          margin-bottom: 10px;
        }

        .cp-auth-title,
        .cp-login-title,
        .cp-auth-card-title {
          margin: 0 !important;
          color: var(--cp-text) !important;
          font-size: clamp(36px, 5vw, 62px) !important;
          line-height: 1.04 !important;
          font-weight: 780 !important;
        }

        .cp-login-title span,
        .cp-auth-card-title span {
          display: block;
        }

        .cp-auth-subtitle,
        .cp-login-subtitle,
        .cp-auth-card-copy,
        .cp-auth-showcase p,
        .cp-login-english {
          max-width: 620px;
          margin: 14px 0 0 !important;
          color: var(--cp-muted) !important;
          font-size: 16px !important;
          line-height: 1.65 !important;
          font-weight: 450 !important;
        }

        .cp-auth-feature-list {
          display: grid;
          gap: 10px;
          margin-top: 28px;
        }

        .cp-auth-feature {
          display: grid;
          grid-template-columns: 36px minmax(0, 1fr);
          gap: 12px;
          align-items: start;
          padding: 14px 0;
          border-bottom: 1px solid var(--cp-line);
        }

        .cp-auth-feature:last-child {
          border-bottom: 0;
        }

        .cp-auth-feature-icon {
          display: grid;
          place-items: center;
          width: 32px;
          height: 32px;
          border-radius: var(--cp-radius-pill);
          background: var(--cp-brand-soft);
          color: var(--cp-brand-dark) !important;
          font-size: 12px;
          font-weight: 740;
        }

        .cp-auth-feature strong {
          display: block;
          color: var(--cp-text) !important;
          font-size: 15px;
          font-weight: 720;
        }

        .cp-auth-feature span {
          display: block;
          margin-top: 3px;
          color: var(--cp-muted) !important;
          font-size: 13px;
          line-height: 1.5;
        }

        .cp-auth-badges,
        .cp-auth-showcase-note {
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
          margin-top: 22px;
        }

        .cp-auth-badges span,
        .cp-auth-showcase-pill {
          display: inline-flex;
          align-items: center;
          min-height: 30px;
          padding: 6px 11px;
          border: 1px solid var(--cp-line);
          border-radius: var(--cp-radius-pill);
          background: var(--cp-surface);
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-weight: 650;
        }

        .cp-auth-spotlight,
        .cp-auth-orbit,
        .cp-weekly-focus {
          display: none !important;
        }

        .cp-auth-tabs-gap {
          height: 10px;
        }

        .cp-auth-form-note {
          color: var(--cp-muted) !important;
          font-size: 13px !important;
        }

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
          }

          .cp-topbar-chips {
            justify-content: flex-start;
          }

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

          .cp-auth-hero,
          .cp-auth-showcase-inner {
            grid-template-columns: 1fr;
            gap: 24px;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
