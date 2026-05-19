from __future__ import annotations

import streamlit as st


def inject_global_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
          --cp-bg: #F7F9F5;
          --cp-bg-2: #FBFCF8;
          --cp-bg-3: #EEF6F1;
          --cp-ink: #12231E;
          --cp-text: #182B24;
          --cp-text-2: #2B3A34;
          --cp-muted: #68766F;
          --cp-muted-2: #97A29D;
          --cp-brand: #0F6B57;
          --cp-brand-deep: #0B4D40;
          --cp-brand-muted: #2F7D69;
          --cp-mint-1: #EEF8F3;
          --cp-mint-2: #E3F2EB;
          --cp-mint-3: #D5EAE0;
          --cp-accent: #FF5A5F;
          --cp-accent-soft: #FFE8EA;
          --cp-warn: #F59E0B;
          --cp-glass: rgba(255,255,255,0.60);
          --cp-glass-strong: rgba(255,255,255,0.78);
          --cp-glass-light: rgba(255,255,255,0.44);
          --cp-glass-soft: var(--cp-glass-light);
          --cp-line: rgba(24,43,36,0.08);
          --cp-line-strong: rgba(24,43,36,0.12);
          --cp-border: rgba(255,255,255,0.64);
          --cp-border-2: var(--cp-line);
          --cp-border-3: rgba(15,107,87,0.15);
          --cp-shadow: 0 22px 70px rgba(18,45,36,0.09);
          --cp-shadow-soft: 0 16px 45px rgba(18,45,36,0.07);
          --cp-shadow-card: 0 22px 65px rgba(18,45,36,0.09);
          --cp-radius-xl: 32px;
          --cp-radius-lg: 26px;
          --cp-radius-md: 18px;
          --cp-radius-pill: 999px;
          --cp-font: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
          --cp-primary: var(--cp-brand);
          --cp-primary-dark: var(--cp-brand-deep);
          --cp-primary-soft: var(--cp-mint-1);
          --cp-brand-2: var(--cp-brand-muted);
          --cp-brand-soft: var(--cp-mint-3);
          --cp-brand-soft-2: var(--cp-mint-1);
          --cp-card: var(--cp-glass);
          --cp-soft: var(--cp-muted-2);
          --cp-danger: var(--cp-accent);
          --cp-danger-soft: var(--cp-accent-soft);
          --cp-warning: var(--cp-warn);
          --cp-warning-soft: #FFF4E5;
          --cp-radius: 28px;
          --cp-sidebar-width: 300px;
          --cp-sidebar-gap: 12px;
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
          background:
            radial-gradient(circle at 8% 4%, rgba(199,236,221,0.55) 0, rgba(199,236,221,0.28) 18rem, transparent 34rem),
            radial-gradient(circle at 88% 6%, rgba(232,245,240,0.75) 0, rgba(232,245,240,0.32) 16rem, transparent 32rem),
            radial-gradient(circle at 10% 92%, rgba(255,231,222,0.32) 0, rgba(255,231,222,0.20) 14rem, transparent 31rem),
            linear-gradient(135deg, #F6F8F4 0%, #FBFCF8 45%, #EEF5EF 100%) !important;
        }

        .stApp::before,
        .stApp::after {
          content: "";
          position: fixed;
          pointer-events: none;
          z-index: 0;
          border-radius: 999px;
          filter: blur(92px);
        }

        .stApp::before {
          width: 34rem;
          height: 34rem;
          left: -11rem;
          top: -12rem;
          background: rgba(199,236,221,0.55);
        }

        .stApp::after {
          width: 32rem;
          height: 32rem;
          right: -9rem;
          top: -10rem;
          background: rgba(232,245,240,0.75);
        }

        [data-testid="stAppViewContainer"],
        section[data-testid="stMain"],
        div[data-testid="stMainBlockContainer"] {
          position: relative;
          z-index: 1;
          background: transparent !important;
        }

        .block-container {
          max-width: 1240px;
          padding: 2.25rem 2.4rem 4rem !important;
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
          font-weight: 800 !important;
        }

        [data-testid="stMarkdownContainer"] p,
        [data-testid="stMarkdownContainer"] li,
        label,
        p {
          color: var(--cp-text-2) !important;
          font-size: 15px;
          line-height: 1.65;
          font-weight: 480;
        }

        small,
        [data-testid="stCaptionContainer"],
        [data-testid="stMarkdownContainer"] small {
          color: var(--cp-muted) !important;
          font-size: 13px !important;
          line-height: 1.55 !important;
        }

        section[data-testid="stSidebar"],
        [data-testid="stSidebar"] {
          width: var(--cp-sidebar-width) !important;
          min-width: var(--cp-sidebar-width) !important;
          padding: 12px 0 12px 12px !important;
          border-right: 0 !important;
          background: transparent !important;
          overflow: visible !important;
        }

        section[data-testid="stSidebar"] > div:first-child,
        [data-testid="stSidebar"] > div:first-child {
          width: var(--cp-sidebar-width) !important;
          min-width: var(--cp-sidebar-width) !important;
          height: 100vh !important;
          background: transparent !important;
          box-shadow: none !important;
          border: 0 !important;
          overflow: visible !important;
        }

        [data-testid="stSidebarContent"] {
          width: calc(var(--cp-sidebar-width) - var(--cp-sidebar-gap)) !important;
          min-width: calc(var(--cp-sidebar-width) - var(--cp-sidebar-gap)) !important;
          height: calc(100vh - 24px) !important;
          max-height: calc(100vh - 24px) !important;
          margin: 0 !important;
          padding: 18px 14px 22px !important;
          border: 1px solid rgba(255,255,255,0.66) !important;
          border-left: 0 !important;
          border-radius: 0 28px 28px 0 !important;
          background: rgba(255,255,255,0.46) !important;
          backdrop-filter: blur(22px) saturate(160%) !important;
          -webkit-backdrop-filter: blur(22px) saturate(160%) !important;
          box-shadow: 0 18px 50px rgba(15,55,43,0.075) !important;
          overflow-y: auto !important;
          overflow-x: hidden !important;
          overscroll-behavior: contain !important;
          scrollbar-width: thin !important;
          scrollbar-color: rgba(15,107,87,0.30) transparent !important;
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar {
          width: 6px;
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar-track {
          background: transparent;
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar-thumb {
          border-radius: 999px;
          background: rgba(15,107,87,0.26);
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar-thumb:hover {
          background: rgba(15,107,87,0.38);
        }

        section[data-testid="stSidebar"][aria-expanded="false"],
        [data-testid="stSidebar"][aria-expanded="false"] {
          width: 0 !important;
          min-width: 0 !important;
          padding: 0 !important;
          margin: 0 !important;
          border: 0 !important;
          overflow: visible !important;
          background: transparent !important;
          box-shadow: none !important;
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
          box-shadow: none !important;
          background: transparent !important;
          pointer-events: none !important;
        }

        [data-testid="collapsedControl"] {
          display: flex !important;
          align-items: center !important;
          justify-content: center !important;
          visibility: visible !important;
          opacity: 1 !important;
          pointer-events: auto !important;
          z-index: 999999 !important;
        }

        [data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
          gap: 0.75rem !important;
        }

        .cp-sidebar-brand,
        .cp-sidebar-status-card,
        .cp-sidebar-toolbox,
        .cp-sidebar-profile-card,
        .cp-sidebar-status {
          border: 1px solid rgba(255,255,255,0.68) !important;
          border-radius: 24px !important;
          background: rgba(255,255,255,0.50) !important;
          backdrop-filter: blur(18px) saturate(150%) !important;
          -webkit-backdrop-filter: blur(18px) saturate(150%) !important;
          box-shadow: 0 12px 30px rgba(18,45,36,0.055) !important;
        }

        .cp-sidebar-brand {
          padding: 16px !important;
          margin-bottom: 10px !important;
        }

        .cp-sidebar-brand-row {
          display: flex;
          align-items: center;
          gap: 13px;
        }

        .cp-logo-mark {
          display: grid;
          place-items: center;
          width: 48px;
          height: 48px;
          flex: 0 0 48px;
          border-radius: 16px;
          background: var(--cp-brand);
          color: #fff;
          font-size: 15px;
          font-weight: 850;
          box-shadow: 0 14px 28px rgba(11,95,74,0.18);
        }

        .cp-sidebar-brand strong {
          display: block;
          color: var(--cp-text);
          font-size: 19px;
          line-height: 1.16;
          font-weight: 800;
        }

        .cp-sidebar-brand em {
          display: block;
          margin-top: 3px;
          color: var(--cp-muted-2);
          font-size: 12px;
          font-style: normal;
          line-height: 1.4;
          font-weight: 650;
        }

        .cp-sidebar-status-card {
          padding: 16px 16px !important;
          margin-bottom: 12px !important;
        }

        .cp-sidebar-status-row {
          display: grid;
          grid-template-columns: 74px minmax(0, 1fr);
          gap: 10px !important;
          align-items: center;
          min-height: 28px !important;
        }

        .cp-sidebar-status-row span {
          color: var(--cp-muted-2) !important;
          font-size: 12px;
          font-weight: 700;
        }

        .cp-sidebar-status-row strong {
          overflow: hidden;
          color: var(--cp-text) !important;
          font-size: 14px;
          font-weight: 760;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        .cp-sidebar-block-title,
        .cp-sidebar-toolbox-title {
          margin: 14px 6px 6px;
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-weight: 800;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] {
          padding: 0 !important;
          border: 0 !important;
          background: transparent !important;
        }

        [data-testid="stSidebar"] [data-baseweb="radio"],
        [data-testid="stSidebar"] [role="radiogroup"] {
          background: transparent !important;
        }

        [data-testid="stSidebar"] [role="radiogroup"] input[type="radio"],
        [data-testid="stSidebar"] [role="radiogroup"] svg,
        [data-testid="stSidebar"] [data-baseweb="radio"] > div:first-child,
        [data-testid="stSidebar"] label [data-baseweb="radio"] > div:first-child {
          display: none !important;
          opacity: 0 !important;
          width: 0 !important;
          height: 0 !important;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] {
          display: grid !important;
          gap: 6px !important;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label,
        [data-testid="stSidebar"] label[data-baseweb="radio"] {
          position: relative !important;
          display: flex !important;
          align-items: center !important;
          min-height: 42px !important;
          margin: 0 !important;
          padding: 0 14px 0 34px !important;
          border: 1px solid transparent !important;
          border-radius: 999px !important;
          background: transparent !important;
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          font-weight: 680 !important;
          line-height: 1.2 !important;
          transition: background .18s ease, color .18s ease, border-color .18s ease, box-shadow .18s ease;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label::before,
        [data-testid="stSidebar"] label[data-baseweb="radio"]::before {
          content: "" !important;
          position: absolute !important;
          left: 13px !important;
          top: 50% !important;
          width: 4px !important;
          height: 18px !important;
          border: 0 !important;
          border-radius: 999px !important;
          transform: translateY(-50%) !important;
          background: transparent !important;
          box-shadow: none !important;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label:hover,
        [data-testid="stSidebar"] label[data-baseweb="radio"]:hover {
          background: rgba(255,255,255,0.48) !important;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked),
        [data-testid="stSidebar"] label[data-baseweb="radio"]:has(input:checked) {
          border-color: rgba(15,107,87,0.12) !important;
          background: rgba(238,248,243,0.86) !important;
          color: var(--cp-brand-deep) !important;
          font-weight: 760 !important;
          box-shadow: inset 0 1px 0 rgba(255,255,255,0.80), 0 8px 20px rgba(15,55,43,0.045) !important;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked)::before,
        [data-testid="stSidebar"] label[data-baseweb="radio"]:has(input:checked)::before {
          background: var(--cp-accent) !important;
          box-shadow: 0 0 0 4px rgba(255,90,95,0.10) !important;
        }

        [data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) * {
          color: var(--cp-brand-deep) !important;
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

        .cp-sidebar-toolbox {
          margin-top: 12px;
          padding: 16px !important;
        }

        .cp-topbar {
          min-height: 92px;
          margin: 0 0 22px;
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 24px;
          padding: 6px 0 22px;
          border-bottom: 1px solid rgba(20,35,30,0.06);
        }

        .cp-topbar h1 {
          margin: 0;
          color: var(--cp-text) !important;
          font-size: clamp(34px, 3vw, 40px);
          line-height: 1.12;
          font-weight: 820 !important;
        }

        .cp-topbar p {
          margin: 12px 0 0;
          max-width: 760px;
          color: var(--cp-muted) !important;
          font-size: 17px;
          line-height: 1.6;
          font-weight: 480;
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
          padding-top: 3px;
        }

        .cp-topbar-chip {
          display: inline-flex;
          align-items: center;
          min-height: 38px;
          padding: 10px 16px;
          border: 1px solid rgba(255,255,255,0.65);
          border-radius: var(--cp-radius-pill);
          background: rgba(255,255,255,0.62);
          backdrop-filter: blur(18px);
          -webkit-backdrop-filter: blur(18px);
          box-shadow: 0 10px 28px rgba(15,55,43,0.055);
          color: var(--cp-brand) !important;
          font-size: 13px;
          font-weight: 760;
          white-space: nowrap;
        }

        .cp-workspace-card,
        .cp-panel,
        .cp-result-panel,
        .cp-decision-card,
        .cp-empty-state,
        .cp-revision-card,
        .cp-job-card,
        .cp-note-card,
        .cp-score-card,
        .cp-score-pill,
        [data-testid="stVerticalBlockBorderWrapper"],
        [data-testid="stMetric"],
        [data-testid="stAlert"] {
          border: 1px solid var(--cp-border) !important;
          border-radius: 28px !important;
          background: var(--cp-glass) !important;
          backdrop-filter: blur(24px) saturate(165%);
          -webkit-backdrop-filter: blur(24px) saturate(165%);
          box-shadow: var(--cp-shadow-card) !important;
        }

        [data-testid="stVerticalBlockBorderWrapper"] > div {
          padding: 28px 30px !important;
        }

        .cp-workspace-card,
        .cp-panel,
        .cp-result-panel {
          margin: 0 0 12px;
          padding: 28px 30px;
        }

        .cp-workspace-card-head h2,
        .cp-panel-head h3,
        .cp-section-title-block h2 {
          margin: 0;
          color: var(--cp-text) !important;
          font-size: clamp(22px, 2vw, 28px);
          line-height: 1.22;
          font-weight: 800 !important;
        }

        .cp-workspace-card-head p,
        .cp-panel-head p,
        .cp-section-title-block p {
          margin: 8px 0 0;
          color: var(--cp-muted) !important;
          font-size: 15px;
          line-height: 1.6;
        }

        .cp-section-title-block {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 18px;
          margin: 0 0 16px;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) {
          display: inline-flex;
          width: auto;
          margin: 0 0 18px !important;
          padding: 6px !important;
          border: 1px solid var(--cp-border-2) !important;
          border-radius: 22px !important;
          background: rgba(255,255,255,0.55) !important;
          box-shadow: 0 10px 30px rgba(15,55,43,0.06) !important;
          backdrop-filter: blur(18px) saturate(150%);
          -webkit-backdrop-filter: blur(18px) saturate(150%);
        }

        [data-testid="stRadio"] div[role="radiogroup"] {
          gap: 4px !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label {
          min-height: 44px !important;
          padding: 0 22px !important;
          border: 0 !important;
          border-radius: 16px !important;
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          font-weight: 700 !important;
          transition: background .18s ease, color .18s ease, transform .18s ease;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label:has(input:checked) {
          background: var(--cp-brand) !important;
          color: #fff !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label:has(input:checked) * {
          color: #fff !important;
        }

        div[data-baseweb="tab-list"],
        .stTabs [data-baseweb="tab-list"] {
          gap: 4px;
          width: fit-content;
          padding: 6px;
          border: 1px solid var(--cp-border-2);
          border-radius: 22px;
          background: rgba(255,255,255,0.55);
          box-shadow: 0 10px 30px rgba(15,55,43,0.06);
          backdrop-filter: blur(18px) saturate(150%);
          -webkit-backdrop-filter: blur(18px) saturate(150%);
        }

        div[data-baseweb="tab"],
        .stTabs [data-baseweb="tab"] {
          min-height: 44px;
          padding: 0 22px;
          border-radius: 16px;
          color: var(--cp-text-2) !important;
          font-size: 14px;
          font-weight: 700;
        }

        div[data-baseweb="tab"][aria-selected="true"],
        .stTabs [aria-selected="true"] {
          background: var(--cp-brand) !important;
          color: #fff !important;
        }

        div[data-baseweb="tab-highlight"] {
          display: none;
        }

        div.stButton > button,
        div[data-testid="stDownloadButton"] button,
        [data-testid="stFormSubmitButton"] button {
          min-height: 46px !important;
          padding: 0 22px !important;
          border-radius: var(--cp-radius-pill) !important;
          border: 1px solid rgba(11,95,74,0.16) !important;
          background: rgba(255,255,255,0.64) !important;
          color: var(--cp-brand) !important;
          box-shadow: none !important;
          font-size: 14px !important;
          font-weight: 760 !important;
          transition: transform .16s ease, background .16s ease, box-shadow .16s ease, border-color .16s ease;
        }

        div.stButton > button[kind="primary"],
        [data-testid="stFormSubmitButton"] button[kind="primary"] {
          min-height: 52px !important;
          border-color: transparent !important;
          background: var(--cp-brand) !important;
          color: #fff !important;
          box-shadow: 0 14px 30px rgba(11,95,74,0.22) !important;
        }

        div.stButton > button:hover,
        div[data-testid="stDownloadButton"] button:hover,
        [data-testid="stFormSubmitButton"] button:hover {
          transform: translateY(-1px);
          border-color: rgba(11,95,74,0.24) !important;
          background: var(--cp-brand-soft-2) !important;
          color: var(--cp-brand) !important;
        }

        div.stButton > button[kind="primary"]:hover,
        [data-testid="stFormSubmitButton"] button[kind="primary"]:hover {
          background: var(--cp-brand-2) !important;
          color: #fff !important;
          box-shadow: 0 16px 34px rgba(11,95,74,0.26) !important;
        }

        [data-testid="stTextInput"] input,
        [data-testid="stNumberInput"] input,
        [data-testid="stTextArea"] textarea,
        div[data-baseweb="select"] > div {
          border: 1px solid var(--cp-border-2) !important;
          border-radius: 18px !important;
          background: rgba(255,255,255,0.72) !important;
          color: var(--cp-text) !important;
          box-shadow: none !important;
          font-size: 15px !important;
          font-family: var(--cp-font) !important;
        }

        [data-testid="stTextInput"] input,
        [data-testid="stNumberInput"] input,
        div[data-baseweb="select"] > div {
          min-height: 48px !important;
        }

        [data-testid="stTextArea"] textarea {
          min-height: 280px !important;
          padding: 20px !important;
          border-radius: 24px !important;
          line-height: 1.65 !important;
          resize: vertical;
        }

        [data-testid="stTextInput"] input::placeholder,
        [data-testid="stTextArea"] textarea::placeholder {
          color: #9AA5A0 !important;
          opacity: 1 !important;
        }

        [data-testid="stTextInput"] input:focus,
        [data-testid="stNumberInput"] input:focus,
        [data-testid="stTextArea"] textarea:focus,
        div[data-baseweb="select"] > div:focus-within {
          border-color: rgba(11,95,74,0.42) !important;
          box-shadow: 0 0 0 4px rgba(11,95,74,0.08) !important;
          outline: none !important;
        }

        [data-testid="stWidgetLabel"] label,
        [data-testid="stWidgetLabel"] p {
          color: var(--cp-text-2) !important;
          font-weight: 700 !important;
        }

        [data-testid="stCheckbox"] label {
          min-height: 30px !important;
          gap: 8px !important;
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
        }

        [data-testid="stExpander"] {
          overflow: hidden;
          border: 1px solid var(--cp-border-2) !important;
          border-radius: 20px !important;
          background: rgba(255,255,255,0.50) !important;
          box-shadow: none !important;
          backdrop-filter: blur(18px) saturate(150%);
          -webkit-backdrop-filter: blur(18px) saturate(150%);
        }

        [data-testid="stExpander"] details summary {
          min-height: 50px !important;
          padding: 0 16px !important;
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          font-weight: 760 !important;
        }

        [data-testid="stExpander"]:hover {
          background: rgba(255,255,255,0.72) !important;
        }

        [data-testid="stFileUploader"] {
          padding: 14px !important;
          border: 1px dashed rgba(11,95,74,0.22) !important;
          border-radius: 22px !important;
          background: rgba(234,245,240,0.38) !important;
          transition: background .18s ease;
        }

        [data-testid="stFileUploader"]:hover {
          background: rgba(234,245,240,0.58) !important;
        }

        [data-testid="stFileUploader"] section {
          padding: 12px !important;
          border: 0 !important;
          background: transparent !important;
        }

        [data-testid="stDataFrame"],
        [data-testid="stTable"] {
          overflow: hidden;
          border: 1px solid rgba(20,35,30,0.06) !important;
          border-radius: 18px !important;
          background: rgba(255,255,255,0.62) !important;
          box-shadow: var(--cp-shadow-soft) !important;
        }

        .cp-empty-state {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 14px;
          width: min(90%, 420px);
          min-height: 178px;
          margin: 18px auto;
          padding: 28px;
          border-radius: 26px !important;
          background: rgba(255,255,255,0.74) !important;
          border: 1px solid rgba(255,255,255,0.70) !important;
          box-shadow: 0 16px 45px rgba(15,55,43,0.08) !important;
        }

        .cp-empty-compact {
          min-height: 150px;
        }

        .cp-empty-icon {
          display: grid;
          place-items: center;
          width: 44px;
          height: 44px;
          flex: 0 0 44px;
          border-radius: 999px;
          background: var(--cp-brand-soft-2);
          color: var(--cp-brand);
          font-size: 0;
          font-weight: 900;
          line-height: 1;
        }

        .cp-empty-icon::after {
          content: "";
          width: 10px;
          height: 10px;
          border-radius: 50%;
          background: var(--cp-brand);
        }

        .cp-empty-state strong {
          display: block;
          margin: 0 0 6px;
          color: var(--cp-text) !important;
          font-size: 19px;
          font-weight: 800;
        }

        .cp-empty-state p {
          margin: 0;
          color: var(--cp-muted) !important;
          font-size: 15px;
          line-height: 1.6;
        }

        .cp-score-grid {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 12px;
          margin: 6px 0 16px;
        }

        .cp-score-card,
        .cp-score-pill {
          min-width: 0;
          padding: 16px 17px;
          border-radius: 22px !important;
        }

        .cp-score-card span,
        .cp-score-pill span,
        .cp-decision-card span,
        .cp-subsection-label {
          display: block;
          color: var(--cp-muted) !important;
          font-size: 12px;
          font-weight: 800;
        }

        .cp-score-card strong,
        .cp-score-pill strong {
          display: block;
          margin-top: 5px;
          color: var(--cp-text) !important;
          font-size: 30px;
          line-height: 1;
          font-weight: 850;
        }

        .cp-score-card em,
        .cp-score-pill em,
        .cp-risk-tag,
        .cp-job-card-meta em {
          display: inline-flex;
          align-items: center;
          min-height: 26px;
          margin-top: 8px;
          padding: 3px 10px;
          border-radius: var(--cp-radius-pill);
          background: var(--cp-brand-soft-2);
          color: var(--cp-brand) !important;
          font-size: 12px;
          font-style: normal;
          font-weight: 760;
        }

        .cp-score-low strong {
          color: var(--cp-accent) !important;
        }

        .cp-score-high strong {
          color: var(--cp-brand) !important;
        }

        .cp-decision-card {
          padding: 20px 22px;
          margin: 0 0 14px;
        }

        .cp-decision-card strong {
          display: block;
          margin-top: 6px;
          color: var(--cp-text) !important;
          font-size: 20px;
          line-height: 1.35;
          font-weight: 830;
        }

        .cp-decision-card p {
          margin: 8px 0 0;
          color: var(--cp-muted) !important;
          font-size: 14px;
        }

        .cp-risk-tag-danger {
          background: var(--cp-accent-soft);
          color: var(--cp-accent) !important;
        }

        .cp-risk-tag-warning {
          background: #FFF4E5;
          color: var(--cp-warn) !important;
        }

        .cp-risk-tag-success,
        .cp-risk-tag-info {
          background: var(--cp-brand-soft-2);
          color: var(--cp-brand) !important;
        }

        .cp-action-list {
          display: grid;
          gap: 8px;
        }

        .cp-action-row {
          display: grid;
          grid-template-columns: 28px minmax(0, 1fr);
          gap: 11px;
          align-items: start;
          padding: 12px 0;
          border-top: 1px solid rgba(20,35,30,0.06);
        }

        .cp-action-row span {
          display: grid;
          place-items: center;
          width: 26px;
          height: 26px;
          border-radius: 999px;
          background: var(--cp-brand-soft-2);
          color: var(--cp-brand);
          font-size: 12px;
          font-weight: 820;
        }

        .cp-action-row p,
        .cp-job-card p {
          margin: 0;
          color: var(--cp-muted) !important;
          font-size: 14px;
          line-height: 1.6;
        }

        .cp-job-card,
        .cp-revision-card,
        .cp-note-card {
          padding: 18px 20px;
          margin-bottom: 12px;
          transition: transform .16s ease, box-shadow .16s ease;
        }

        .cp-job-card:hover {
          transform: translateY(-2px);
          box-shadow: 0 24px 70px rgba(15,55,43,0.12) !important;
        }

        .cp-job-card-top {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 12px;
        }

        .cp-job-card-top strong {
          color: var(--cp-text) !important;
          font-size: 16px;
          font-weight: 800;
        }

        .cp-job-card-top span {
          color: var(--cp-brand);
          font-size: 26px;
          font-weight: 850;
          line-height: 1;
        }

        .cp-auth-hero {
          display: none !important;
        }

        .cp-auth-showcase,
        .cp-auth-card {
          position: relative;
          z-index: 1;
        }

        .cp-auth-showcase {
          min-height: calc(100vh - 7rem);
          display: flex;
          align-items: center;
          padding: 20px 0;
        }

        .cp-auth-showcase-inner {
          width: 100%;
        }

        .cp-hero-eyebrow,
        .cp-auth-showcase-kicker,
        .cp-auth-card-kicker {
          display: inline-flex;
          align-items: center;
          min-height: 32px;
          padding: 0 13px;
          border: 1px solid rgba(11,95,74,0.12);
          border-radius: var(--cp-radius-pill);
          background: rgba(234,245,240,0.72);
          color: var(--cp-brand) !important;
          font-size: 12px;
          font-weight: 800;
          text-transform: uppercase;
        }

        .cp-login-title,
        .cp-auth-showcase h3 {
          margin: 18px 0 0;
          max-width: 720px;
          color: var(--cp-text) !important;
          font-size: clamp(56px, 5.6vw, 68px);
          line-height: 1.07;
          font-weight: 850 !important;
        }

        .cp-login-title .cp-gradient {
          background: linear-gradient(135deg, #083F33 0%, #0B5F4A 54%, #2A806E 100%);
          -webkit-background-clip: text;
          background-clip: text;
          color: transparent;
        }

        .cp-login-subtitle {
          max-width: 680px;
          margin: 22px 0 0;
          color: var(--cp-text-2) !important;
          font-size: 18px;
          line-height: 1.7;
          font-weight: 520;
        }

        .cp-login-english {
          margin: 14px 0 0;
          color: var(--cp-muted) !important;
          font-size: 15px;
          font-weight: 760;
        }

        .cp-weekly-focus {
          max-width: 560px;
          margin: 28px 0 0;
          padding: 18px 20px;
          border: 1px solid var(--cp-border);
          border-radius: var(--cp-radius-lg);
          background: rgba(255,255,255,0.54);
          backdrop-filter: blur(22px) saturate(160%);
          -webkit-backdrop-filter: blur(22px) saturate(160%);
          box-shadow: var(--cp-shadow-soft);
        }

        .cp-weekly-focus span {
          color: var(--cp-muted-2);
          font-size: 12px;
          font-weight: 800;
          text-transform: uppercase;
        }

        .cp-weekly-focus strong {
          display: block;
          margin-top: 6px;
          color: var(--cp-text) !important;
          font-size: 16px;
          line-height: 1.55;
          font-weight: 780;
        }

        .cp-auth-showcase h4 {
          margin: 34px 0 0;
          color: var(--cp-text) !important;
          font-size: 28px;
          line-height: 1.2;
          font-weight: 820;
        }

        .cp-auth-showcase-copy-line {
          display: block;
          max-width: 720px;
          color: var(--cp-muted) !important;
          font-size: 16px;
          line-height: 1.72;
        }

        .cp-auth-feature-list {
          display: grid;
          grid-template-columns: repeat(3, minmax(0, 1fr));
          gap: 14px;
          margin-top: 24px;
        }

        .cp-auth-feature {
          min-height: 210px;
          padding: 20px;
          border: 1px solid var(--cp-border);
          border-radius: var(--cp-radius-lg);
          background: rgba(255,255,255,0.56);
          backdrop-filter: blur(24px) saturate(165%);
          -webkit-backdrop-filter: blur(24px) saturate(165%);
          box-shadow: var(--cp-shadow-card);
        }

        .cp-auth-feature-icon {
          margin-bottom: 28px;
          color: var(--cp-brand);
          font-size: 13px;
          font-weight: 850;
        }

        .cp-auth-feature strong {
          display: block;
          color: var(--cp-text) !important;
          font-size: 17px;
          line-height: 1.35;
          font-weight: 800;
        }

        .cp-auth-feature span {
          display: block;
          margin-top: 10px;
          color: var(--cp-muted) !important;
          font-size: 14px;
          line-height: 1.72;
        }

        .cp-auth-showcase-note {
          display: none;
        }

        .cp-auth-card {
          width: min(100%, 540px);
          margin: calc((100vh - 650px) / 2) 0 calc((100vh - 650px) / 2) auto;
          padding: 42px;
          border: 1px solid rgba(255,255,255,0.68);
          border-radius: var(--cp-radius-xl);
          background: rgba(255,255,255,0.70);
          backdrop-filter: blur(28px) saturate(170%);
          -webkit-backdrop-filter: blur(28px) saturate(170%);
          box-shadow: 0 24px 80px rgba(15,55,43,0.12);
        }

        .cp-auth-card-title {
          margin: 18px 0 0;
          color: var(--cp-text) !important;
          font-size: clamp(36px, 3.3vw, 44px);
          line-height: 1.12;
          font-weight: 850 !important;
        }

        .cp-auth-card-title span {
          display: inline;
        }

        .cp-auth-title-break::before {
          content: "";
          display: block;
        }

        .cp-auth-card-copy,
        .cp-auth-form-note {
          margin: 14px 0 0;
          color: var(--cp-muted) !important;
          font-size: 16px !important;
          line-height: 1.6;
        }

        .cp-auth-tabs-gap {
          height: 20px;
        }

        .cp-auth-card .stTabs [data-baseweb="tab-list"] {
          width: 100%;
          display: flex;
          border-radius: var(--cp-radius-pill);
          background: rgba(255,255,255,0.45);
        }

        .cp-auth-card .stTabs [data-baseweb="tab"] {
          flex: 1 1 0;
          justify-content: center;
          border-radius: var(--cp-radius-pill);
        }

        .cp-auth-card [data-testid="stTextInput"] input {
          min-height: 54px !important;
          border-radius: 20px !important;
        }

        .cp-auth-card [data-testid="stFormSubmitButton"] button {
          width: 100% !important;
        }

        .cp-table-toolbar {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 14px;
          margin-bottom: 12px;
          padding: 12px 14px;
          border: 1px solid var(--cp-border);
          border-radius: 18px;
          background: rgba(255,255,255,0.58);
        }

        .cp-compact-metric {
          padding: 16px;
          border: 1px solid var(--cp-border);
          border-radius: 20px;
          background: rgba(255,255,255,0.58);
          box-shadow: var(--cp-shadow-soft);
        }

        .cp-compact-metric span {
          color: var(--cp-muted);
          font-size: 12px;
          font-weight: 760;
        }

        .cp-compact-metric strong {
          display: block;
          margin-top: 4px;
          color: var(--cp-text);
          font-size: 20px;
          font-weight: 820;
        }

        @media (max-width: 1100px) {
          .cp-auth-feature-list {
            grid-template-columns: 1fr;
          }
          .cp-auth-feature {
            min-height: auto;
          }
          .cp-auth-card {
            margin: 24px auto;
          }
        }

        @media (max-width: 900px) {
          .block-container {
            padding: 1.5rem 1rem 2.5rem !important;
          }
          .cp-topbar {
            align-items: flex-start;
            flex-direction: column;
          }
          .cp-topbar-chips {
            justify-content: flex-start;
          }
          .cp-score-grid,
          .cp-auth-feature-list {
            grid-template-columns: 1fr;
          }
          .cp-auth-showcase {
            min-height: auto;
            padding: 24px 0 0;
          }
          .cp-login-title,
          .cp-auth-showcase h3 {
            font-size: 48px;
          }
          .cp-auth-card {
            padding: 30px 24px;
          }
        }
        /* Refinement pass: lighter Apple-like controls, quieter selection states. */
        [data-testid="stDecoration"],
        [data-testid="stStatusWidget"],
        [data-testid="stDeployButton"],
        #MainMenu,
        footer,
        .stDeployButton {
          display: none !important;
          visibility: hidden !important;
          pointer-events: none !important;
        }

        header[data-testid="stHeader"] {
          display: block !important;
          visibility: visible !important;
          pointer-events: auto !important;
          background: transparent !important;
          border: 0 !important;
          box-shadow: none !important;
        }

        [data-testid="stToolbar"] {
          background: transparent !important;
          border: 0 !important;
          box-shadow: none !important;
        }

        [data-testid="collapsedControl"],
        button[data-testid="collapsedControl"],
        header[data-testid="stHeader"] button,
        header[data-testid="stHeader"] [role="button"],
        header[data-testid="stHeader"] [data-testid="collapsedControl"],
        header[data-testid="stHeader"] [data-testid="baseButton-headerNoPadding"] {
          visibility: visible !important;
          opacity: 1 !important;
          pointer-events: auto !important;
          z-index: 999999 !important;
        }

        .stApp {
          background:
            radial-gradient(circle at 8% 4%, rgba(199,236,221,0.48) 0, rgba(199,236,221,0.22) 18rem, transparent 34rem),
            radial-gradient(circle at 88% 6%, rgba(232,245,240,0.66) 0, rgba(232,245,240,0.28) 16rem, transparent 32rem),
            radial-gradient(circle at 10% 92%, rgba(255,231,222,0.28) 0, rgba(255,231,222,0.16) 14rem, transparent 31rem),
            linear-gradient(135deg, #F7F9F5 0%, #FBFCF8 45%, #EEF6F1 100%) !important;
        }

        .cp-workspace-card,
        .cp-panel,
        .cp-result-panel,
        .cp-decision-card,
        .cp-empty-state,
        .cp-revision-card,
        .cp-job-card,
        .cp-note-card,
        .cp-score-card,
        .cp-score-pill,
        [data-testid="stVerticalBlockBorderWrapper"],
        [data-testid="stMetric"],
        [data-testid="stAlert"] {
          box-shadow: 0 16px 45px rgba(18,45,36,0.07) !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]),
        div[data-baseweb="tab-list"],
        .stTabs [data-baseweb="tab-list"] {
          padding: 6px !important;
          border: 1px solid var(--cp-line) !important;
          border-radius: var(--cp-radius-pill) !important;
          background: rgba(255,255,255,0.52) !important;
          box-shadow: 0 12px 36px rgba(18,45,36,0.06) !important;
          backdrop-filter: blur(20px) saturate(160%);
          -webkit-backdrop-filter: blur(20px) saturate(160%);
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label,
        div[data-baseweb="tab"],
        .stTabs [data-baseweb="tab"] {
          min-height: 42px !important;
          padding: 0 22px !important;
          border-radius: var(--cp-radius-pill) !important;
          color: #5F6D66 !important;
          font-weight: 650 !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label:has(input:checked),
        div[data-baseweb="tab"][aria-selected="true"],
        .stTabs [aria-selected="true"] {
          border: 1px solid rgba(15,107,87,0.16) !important;
          background: rgba(238,248,243,0.96) !important;
          color: var(--cp-brand-deep) !important;
          box-shadow: inset 0 1px 0 rgba(255,255,255,0.9), 0 8px 22px rgba(18,45,36,0.07) !important;
          font-weight: 750 !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label:has(input:checked) *,
        .stTabs [aria-selected="true"] * {
          color: var(--cp-brand-deep) !important;
        }

        div.stButton > button,
        div.stButton > button[kind="primary"],
        div[data-testid="stDownloadButton"] button,
        [data-testid="stFormSubmitButton"] button,
        [data-testid="stFormSubmitButton"] button[kind="primary"] {
          display: inline-flex !important;
          align-items: center !important;
          justify-content: center !important;
          text-align: center !important;
          line-height: 1.2 !important;
          gap: 0.4rem !important;
          min-height: 48px !important;
          padding: 0 24px !important;
          border: 1px solid rgba(15,107,87,0.18) !important;
          border-radius: var(--cp-radius-pill) !important;
          background: linear-gradient(180deg, rgba(255,255,255,0.92), rgba(238,248,243,0.86)) !important;
          color: var(--cp-brand-deep) !important;
          box-shadow: 0 10px 26px rgba(18,45,36,0.08), inset 0 1px 0 rgba(255,255,255,0.85) !important;
          font-weight: 750 !important;
          transition: all .18s ease !important;
        }

        div.stButton > button p,
        div[data-testid="stDownloadButton"] button p,
        [data-testid="stFormSubmitButton"] button p,
        div.stButton > button span,
        div[data-testid="stDownloadButton"] button span,
        [data-testid="stFormSubmitButton"] button span {
          margin: 0 !important;
          line-height: 1.2 !important;
          text-align: center !important;
        }

        div.stButton > button:hover,
        div.stButton > button[kind="primary"]:hover,
        div[data-testid="stDownloadButton"] button:hover,
        [data-testid="stFormSubmitButton"] button:hover,
        [data-testid="stFormSubmitButton"] button[kind="primary"]:hover {
          transform: translateY(-1px);
          border-color: rgba(15,107,87,0.28) !important;
          background: linear-gradient(180deg, rgba(255,255,255,1), rgba(227,242,235,0.92)) !important;
          color: var(--cp-brand-deep) !important;
          box-shadow: 0 14px 32px rgba(18,45,36,0.11), inset 0 1px 0 rgba(255,255,255,0.9) !important;
        }

        div.stButton > button:active,
        [data-testid="stFormSubmitButton"] button:active {
          transform: translateY(0);
          box-shadow: 0 6px 18px rgba(18,45,36,0.08) !important;
        }

        [data-testid="stTextInput"] input,
        [data-testid="stNumberInput"] input,
        [data-testid="stTextArea"] textarea,
        div[data-baseweb="select"] > div {
          border-color: var(--cp-line) !important;
          background: rgba(255,255,255,0.78) !important;
          box-shadow: inset 0 1px 0 rgba(255,255,255,0.82) !important;
          color: var(--cp-text) !important;
        }

        [data-testid="stTextInput"] input:focus,
        [data-testid="stNumberInput"] input:focus,
        [data-testid="stTextArea"] textarea:focus,
        div[data-baseweb="select"] > div:focus-within {
          border-color: rgba(15,107,87,0.30) !important;
          box-shadow: 0 0 0 4px rgba(15,107,87,0.07), inset 0 1px 0 rgba(255,255,255,0.9) !important;
        }

        [data-testid="stTextInput"] button,
        [data-testid="stTextInput"] [role="button"] {
          background: rgba(255,255,255,0.35) !important;
          color: var(--cp-muted) !important;
          border: 0 !important;
        }

        [data-testid="stTextInput"] button:hover,
        [data-testid="stTextInput"] [role="button"]:hover {
          color: var(--cp-brand-deep) !important;
          background: rgba(238,248,243,0.62) !important;
        }

        .cp-auth-showcase {
          min-height: calc(100vh - 5rem);
          padding: 8px 0;
        }

        .cp-auth-showcase-kicker,
        .cp-auth-card-kicker {
          height: 34px;
          min-height: 34px;
          padding: 0 16px;
          border: 1px solid rgba(15,107,87,0.12);
          background: rgba(255,255,255,0.58);
          color: var(--cp-brand-deep) !important;
          font-size: 13px;
          font-weight: 760;
          letter-spacing: .04em !important;
        }

        .cp-login-title,
        .cp-auth-showcase h3 {
          margin-top: 20px;
          max-width: 860px;
          line-height: 1;
        }

        .cp-login-title-brand,
        .cp-login-title-cn {
          display: block;
          white-space: nowrap;
        }

        .cp-login-title-brand {
          color: var(--cp-brand-deep);
          font-size: clamp(54px, 6vw, 86px);
          line-height: .95;
          font-weight: 850;
        }

        .cp-login-title-cn {
          margin-top: 6px;
          color: var(--cp-ink);
          font-size: clamp(42px, 4.8vw, 72px);
          line-height: 1.02;
          font-weight: 850;
        }

        .cp-login-subtitle {
          max-width: 720px;
          margin-top: 24px;
          color: var(--cp-text-2) !important;
          font-size: 18px;
          line-height: 1.75;
          font-weight: 560;
        }

        .cp-login-english {
          margin-top: 24px;
          color: var(--cp-muted) !important;
          font-size: 15px;
          font-weight: 700;
        }

        .cp-weekly-focus {
          width: min(100%, 720px);
          max-width: 720px;
          margin-top: 24px;
          padding: 24px 28px;
          border: 1px solid rgba(255,255,255,0.68);
          border-radius: 28px;
          background: rgba(255,255,255,0.58);
          box-shadow: 0 18px 48px rgba(18,45,36,0.08);
          backdrop-filter: blur(24px) saturate(160%);
          -webkit-backdrop-filter: blur(24px) saturate(160%);
        }

        .cp-weekly-focus span {
          color: #7F8C86 !important;
          font-size: 12px;
          letter-spacing: .08em !important;
          font-weight: 800;
        }

        .cp-weekly-focus strong {
          font-size: 18px;
          line-height: 1.55;
          font-weight: 760;
          color: var(--cp-text) !important;
        }

        .cp-auth-showcase h4 {
          margin-top: 28px;
          font-size: 24px;
        }

        .cp-auth-feature-list {
          grid-template-columns: repeat(3, minmax(0, 1fr));
          gap: 18px;
          margin-top: 28px;
        }

        .cp-auth-feature {
          min-height: 152px;
          padding: 22px;
          border: 1px solid rgba(255,255,255,0.66);
          border-radius: 26px;
          background: rgba(255,255,255,0.58);
          box-shadow: 0 16px 42px rgba(18,45,36,0.07);
          backdrop-filter: blur(22px) saturate(160%);
          -webkit-backdrop-filter: blur(22px) saturate(160%);
        }

        .cp-auth-feature-icon {
          margin-bottom: 12px;
          color: var(--cp-brand);
          font-size: 13px;
          font-weight: 820;
        }

        .cp-auth-feature strong {
          font-size: 17px;
          margin-top: 12px;
          color: var(--cp-text) !important;
        }

        .cp-auth-feature span {
          margin-top: 8px;
          color: var(--cp-muted) !important;
          font-size: 14px;
          line-height: 1.65;
        }

        .cp-auth-card {
          width: min(100%, 560px);
          margin: 0 0 0 auto;
          padding: 36px 40px;
          border: 1px solid rgba(255,255,255,0.66);
          border-radius: 34px;
          background: rgba(255,255,255,0.58);
          box-shadow: 0 22px 70px rgba(18,45,36,0.09);
          backdrop-filter: blur(28px) saturate(165%);
          -webkit-backdrop-filter: blur(28px) saturate(165%);
        }

        .cp-auth-card-title {
          max-width: 560px;
          margin-top: 18px;
          color: var(--cp-ink) !important;
          font-size: clamp(32px, 3vw, 46px);
          line-height: 1.12;
          font-weight: 850 !important;
        }

        .cp-auth-card-title span {
          display: block;
        }

        .cp-auth-card-copy {
          margin-top: 18px;
          color: var(--cp-text-2) !important;
          font-size: 17px !important;
          line-height: 1.7;
          font-weight: 560;
        }

        .cp-auth-tabs-gap {
          height: 18px;
          border-top: 1px solid rgba(255,255,255,0.52);
          margin-top: 24px;
          padding-top: 18px;
        }

        .cp-auth-card .stTabs {
          padding: 22px;
          border: 1px solid rgba(255,255,255,0.66);
          border-radius: 30px;
          background: rgba(255,255,255,0.54);
          box-shadow: 0 16px 48px rgba(18,45,36,0.07);
          backdrop-filter: blur(24px) saturate(160%);
          -webkit-backdrop-filter: blur(24px) saturate(160%);
        }

        .cp-auth-card .stTabs [data-baseweb="tab-list"] {
          width: fit-content !important;
          margin-bottom: 20px;
        }

        .cp-auth-card .stTabs [data-baseweb="tab"] {
          flex: 0 0 auto;
        }

        .cp-auth-card [data-testid="stVerticalBlock"] {
          gap: 1.05rem !important;
        }

        .cp-auth-card [data-testid="stWidgetLabel"] label,
        .cp-auth-card [data-testid="stWidgetLabel"] p {
          margin-bottom: 8px;
          color: var(--cp-text) !important;
          font-size: 15px !important;
          font-weight: 760 !important;
        }

        .cp-auth-card [data-testid="stTextInput"] input {
          min-height: 52px !important;
          padding: 0 18px !important;
          border-radius: 18px !important;
          font-size: 16px !important;
        }

        .cp-auth-card [data-testid="stFormSubmitButton"] button {
          margin-top: 10px;
          width: 100% !important;
        }

        .cp-table-toolbar,
        .cp-compact-metric {
          background: rgba(255,255,255,0.64) !important;
          border: 1px solid rgba(255,255,255,0.62) !important;
          border-radius: 26px !important;
          box-shadow: 0 14px 36px rgba(18,45,36,0.06) !important;
        }

        div[data-testid="column"] + div[data-testid="column"] {
          padding-left: 8px;
        }

        @media (max-width: 900px) {
          .cp-login-title-brand {
            font-size: 52px;
          }
          .cp-login-title-cn {
            white-space: normal;
            font-size: 42px;
          }
          .cp-auth-card {
            width: 100%;
            padding: 28px 24px;
          }
          .cp-auth-card .stTabs {
            padding: 18px;
          }
        }

        /* =========================================================
           CareerPilot hotfix
           Sidebar real scroll area + decision page visual polish
           Keep this block at the very end of the global CSS.
           ========================================================= */

        /* ---------- sidebar scroll hotfix ---------- */

        section[data-testid="stSidebar"],
        [data-testid="stSidebar"] {
          height: 100dvh !important;
          max-height: 100dvh !important;
          min-height: 0 !important;
          padding: 12px 0 12px 12px !important;
          overflow: hidden !important;
          pointer-events: auto !important;
        }

        section[data-testid="stSidebar"] > div:first-child,
        [data-testid="stSidebar"] > div:first-child {
          height: 100% !important;
          max-height: 100% !important;
          min-height: 0 !important;
          display: flex !important;
          flex-direction: column !important;
          overflow: hidden !important;
          pointer-events: auto !important;
        }

        [data-testid="stSidebarContent"] {
          width: calc(var(--cp-sidebar-width) - var(--cp-sidebar-gap)) !important;
          min-width: calc(var(--cp-sidebar-width) - var(--cp-sidebar-gap)) !important;
          height: calc(100dvh - 24px) !important;
          max-height: calc(100dvh - 24px) !important;
          min-height: 0 !important;
          margin: 0 !important;
          padding: 18px 14px 22px !important;
          display: flex !important;
          flex-direction: column !important;
          border: 1px solid rgba(255,255,255,0.66) !important;
          border-left: 0 !important;
          border-radius: 0 28px 28px 0 !important;
          background: rgba(255,255,255,0.48) !important;
          backdrop-filter: blur(22px) saturate(160%) !important;
          -webkit-backdrop-filter: blur(22px) saturate(160%) !important;
          box-shadow: 0 18px 50px rgba(15,55,43,0.075) !important;
          overflow: hidden !important;
          pointer-events: auto !important;
        }

        /* Streamlit 新版本常用的真实滚动层 */
        [data-testid="stSidebarUserContent"] {
          flex: 1 1 auto !important;
          min-height: 0 !important;
          height: auto !important;
          max-height: 100% !important;
          padding: 0 4px 24px 0 !important;
          overflow-y: auto !important;
          overflow-x: hidden !important;
          overscroll-behavior: contain !important;
          scrollbar-gutter: stable !important;
          scrollbar-width: thin !important;
          scrollbar-color: rgba(15,107,87,0.34) transparent !important;
          pointer-events: auto !important;
        }

        /* 兼容没有 stSidebarUserContent 的 Streamlit DOM */
        [data-testid="stSidebarContent"]:not(:has([data-testid="stSidebarUserContent"])) {
          overflow-y: auto !important;
          overflow-x: hidden !important;
          overscroll-behavior: contain !important;
          scrollbar-gutter: stable !important;
          scrollbar-width: thin !important;
          scrollbar-color: rgba(15,107,87,0.34) transparent !important;
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar,
        [data-testid="stSidebarUserContent"]::-webkit-scrollbar {
          width: 8px !important;
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar-track,
        [data-testid="stSidebarUserContent"]::-webkit-scrollbar-track {
          background: transparent !important;
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar-thumb,
        [data-testid="stSidebarUserContent"]::-webkit-scrollbar-thumb {
          min-height: 48px !important;
          border: 2px solid transparent !important;
          border-radius: 999px !important;
          background: rgba(15,107,87,0.30) !important;
          background-clip: padding-box !important;
        }

        [data-testid="stSidebarContent"]::-webkit-scrollbar-thumb:hover,
        [data-testid="stSidebarUserContent"]::-webkit-scrollbar-thumb:hover {
          background: rgba(15,107,87,0.48) !important;
          background-clip: padding-box !important;
        }

        [data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
          min-height: max-content !important;
        }

        section[data-testid="stSidebar"][aria-expanded="false"],
        [data-testid="stSidebar"][aria-expanded="false"] {
          width: 0 !important;
          min-width: 0 !important;
          padding: 0 !important;
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
          box-shadow: none !important;
          background: transparent !important;
          pointer-events: none !important;
        }

        /* ---------- decision page polish ---------- */

        .block-container {
          max-width: 1360px !important;
          padding: 2rem min(4vw, 4.2rem) 4rem !important;
        }

        .cp-topbar {
          min-height: unset !important;
          margin: 0 0 28px !important;
          padding: 12px 0 28px !important;
          border-bottom: 1px solid rgba(20,35,30,0.07) !important;
        }

        .cp-topbar h1 {
          font-size: clamp(44px, 4.2vw, 60px) !important;
          line-height: 1.04 !important;
          font-weight: 860 !important;
          letter-spacing: -0.04em !important;
        }

        .cp-topbar p {
          max-width: 780px !important;
          margin-top: 18px !important;
          color: rgba(43,58,52,0.70) !important;
          font-size: clamp(17px, 1.35vw, 22px) !important;
          line-height: 1.55 !important;
          font-weight: 600 !important;
        }

        .cp-topbar-chips {
          gap: 12px !important;
          padding-top: 0 !important;
        }

        .cp-topbar-chip {
          min-height: 48px !important;
          padding: 0 22px !important;
          border-radius: 999px !important;
          background: rgba(255,255,255,0.70) !important;
          box-shadow: 0 16px 42px rgba(18,45,36,0.08) !important;
          color: var(--cp-brand-deep) !important;
          font-size: 15px !important;
          font-weight: 820 !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) {
          margin: 4px 0 34px !important;
          padding: 7px !important;
          border-radius: 999px !important;
          background: rgba(255,255,255,0.62) !important;
          border: 1px solid rgba(24,43,36,0.08) !important;
          box-shadow: 0 18px 44px rgba(18,45,36,0.07) !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) div[role="radiogroup"] {
          display: flex !important;
          gap: 8px !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label {
          min-width: 190px !important;
          min-height: 52px !important;
          justify-content: center !important;
          padding: 0 28px !important;
          border-radius: 999px !important;
          color: rgba(24,43,36,0.82) !important;
          font-size: 16px !important;
          font-weight: 760 !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label:has(input:checked) {
          border: 1px solid rgba(15,107,87,0.16) !important;
          background: rgba(238,248,243,0.98) !important;
          color: var(--cp-brand-deep) !important;
          box-shadow: inset 0 1px 0 rgba(255,255,255,0.92), 0 10px 28px rgba(18,45,36,0.08) !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label:has(input:checked)::before {
          content: "" !important;
          width: 12px !important;
          height: 12px !important;
          margin-right: 10px !important;
          border-radius: 999px !important;
          background: var(--cp-accent) !important;
          box-shadow: 0 0 0 6px rgba(255,90,95,0.12) !important;
        }

        .cp-decision-page {
          width: min(100%, 1080px);
          margin: 0 auto;
        }

        .cp-empty-state {
          width: min(100%, 760px) !important;
          min-height: 230px !important;
          margin: clamp(38px, 6vh, 76px) auto 0 !important;
          padding: 40px 46px !important;
          border-radius: 34px !important;
          background:
            linear-gradient(135deg, rgba(255,255,255,0.86), rgba(247,251,248,0.74)) !important;
          border: 1px solid rgba(255,255,255,0.78) !important;
          box-shadow: 0 26px 78px rgba(18,45,36,0.10) !important;
          backdrop-filter: blur(28px) saturate(170%) !important;
          -webkit-backdrop-filter: blur(28px) saturate(170%) !important;
        }

        .cp-empty-state-decision {
          display: grid !important;
          grid-template-columns: 62px minmax(0, 1fr) !important;
          align-items: center !important;
          justify-content: stretch !important;
          gap: 24px !important;
        }

        .cp-empty-state-decision .cp-empty-icon {
          width: 62px !important;
          height: 62px !important;
          border-radius: 22px !important;
          background: linear-gradient(135deg, rgba(238,248,243,1), rgba(213,234,224,0.88)) !important;
          box-shadow: inset 0 1px 0 rgba(255,255,255,0.88), 0 14px 32px rgba(15,107,87,0.10) !important;
        }

        .cp-empty-state-decision .cp-empty-icon::after {
          width: 14px !important;
          height: 14px !important;
          background: var(--cp-accent) !important;
          box-shadow: 0 0 0 7px rgba(255,90,95,0.14) !important;
        }

        .cp-empty-badge {
          display: inline-flex;
          align-items: center;
          width: fit-content;
          min-height: 30px;
          margin-bottom: 12px;
          padding: 0 12px;
          border-radius: 999px;
          background: rgba(238,248,243,0.96);
          color: var(--cp-brand-deep) !important;
          font-size: 13px;
          font-weight: 820;
        }

        .cp-empty-state-decision strong {
          margin: 0 !important;
          color: var(--cp-text) !important;
          font-size: clamp(24px, 2vw, 30px) !important;
          line-height: 1.22 !important;
          font-weight: 860 !important;
        }

        .cp-empty-state-decision p {
          max-width: 560px;
          margin: 10px 0 0 !important;
          color: rgba(43,58,52,0.70) !important;
          font-size: 17px !important;
          line-height: 1.7 !important;
          font-weight: 560 !important;
        }

        .cp-decision-grid {
          display: grid;
          grid-template-columns: minmax(0, 1.1fr) minmax(320px, 0.9fr);
          gap: 18px;
          align-items: start;
        }

        .cp-decision-card-main,
        .cp-decision-side-card {
          border: 1px solid rgba(255,255,255,0.72);
          border-radius: 32px;
          background: rgba(255,255,255,0.66);
          box-shadow: 0 22px 64px rgba(18,45,36,0.08);
          backdrop-filter: blur(26px) saturate(165%);
          -webkit-backdrop-filter: blur(26px) saturate(165%);
        }

        .cp-decision-card-main {
          padding: 30px 34px;
        }

        .cp-decision-side-card {
          padding: 24px 26px;
        }

        @media (max-width: 900px) {
          .block-container {
            padding: 1.3rem 1rem 3rem !important;
          }

          .cp-topbar h1 {
            font-size: 42px !important;
          }

          .cp-topbar-chips {
            justify-content: flex-start !important;
          }

          [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) {
            width: 100% !important;
          }

          [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) div[role="radiogroup"] {
            width: 100% !important;
            flex-direction: column !important;
          }

          [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label {
            width: 100% !important;
            min-width: 0 !important;
          }

          .cp-empty-state-decision {
            grid-template-columns: 1fr !important;
            text-align: left !important;
          }

          .cp-decision-grid {
            grid-template-columns: 1fr;
          }
        }

        /* =========================================================
           CareerPilot final visual consistency pass
           Sidebar controls + segmented nav cleanup
           Keep this block at the very end of the global CSS.
           ========================================================= */

        /* ---------- global density refinement ---------- */

        .block-container {
          max-width: 1340px !important;
          padding: 2rem min(3.8vw, 4rem) 4rem !important;
        }

        .cp-topbar {
          margin-bottom: 26px !important;
          padding-bottom: 26px !important;
        }

        .cp-topbar h1 {
          letter-spacing: -0.045em !important;
        }

        .cp-topbar-chip {
          min-height: 46px !important;
          padding: 0 22px !important;
          background: rgba(255,255,255,0.68) !important;
          border-color: rgba(255,255,255,0.70) !important;
          box-shadow: 0 12px 34px rgba(18,45,36,0.075) !important;
        }

        /* ---------- sidebar compact controls ---------- */

        [data-testid="stSidebar"] {
          --cp-sidebar-control-radius: 18px;
        }

        [data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
          gap: 0.55rem !important;
        }

        [data-testid="stSidebar"] .cp-sidebar-block-title,
        [data-testid="stSidebar"] .cp-sidebar-toolbox-title {
          margin: 16px 8px 8px !important;
          color: rgba(43,58,52,0.62) !important;
          font-size: 12px !important;
          line-height: 1.2 !important;
          font-weight: 820 !important;
        }

        [data-testid="stSidebar"] [data-testid="stExpander"] {
          margin: 7px 0 !important;
          overflow: hidden !important;
          border: 1px solid rgba(15,107,87,0.10) !important;
          border-radius: var(--cp-sidebar-control-radius) !important;
          background: rgba(255,255,255,0.38) !important;
          box-shadow: none !important;
          backdrop-filter: blur(16px) saturate(145%) !important;
          -webkit-backdrop-filter: blur(16px) saturate(145%) !important;
        }

        [data-testid="stSidebar"] [data-testid="stExpander"]:hover {
          border-color: rgba(15,107,87,0.16) !important;
          background: rgba(255,255,255,0.52) !important;
          box-shadow: 0 8px 22px rgba(18,45,36,0.045) !important;
        }

        [data-testid="stSidebar"] [data-testid="stExpander"] details {
          background: transparent !important;
        }

        [data-testid="stSidebar"] [data-testid="stExpander"] details summary {
          min-height: 46px !important;
          padding: 0 14px !important;
          border-radius: var(--cp-sidebar-control-radius) !important;
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          font-weight: 760 !important;
        }

        [data-testid="stSidebar"] [data-testid="stExpander"] details summary:hover {
          background: rgba(238,248,243,0.58) !important;
        }

        [data-testid="stSidebar"] [data-testid="stExpander"] details summary p,
        [data-testid="stSidebar"] [data-testid="stExpander"] details summary span {
          margin: 0 !important;
          color: var(--cp-text-2) !important;
          font-size: 14px !important;
          line-height: 1.25 !important;
          font-weight: 760 !important;
        }

        [data-testid="stSidebar"] [data-testid="stExpander"] details[open] summary {
          border-bottom: 1px solid rgba(15,107,87,0.08) !important;
          border-radius: var(--cp-sidebar-control-radius) var(--cp-sidebar-control-radius) 0 0 !important;
          background: rgba(238,248,243,0.52) !important;
        }

        [data-testid="stSidebar"] [data-testid="stExpanderDetails"] {
          padding: 12px 14px 14px !important;
          background: rgba(255,255,255,0.28) !important;
        }

        [data-testid="stSidebar"] [data-testid="stCaptionContainer"],
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
        [data-testid="stSidebar"] small {
          color: rgba(43,58,52,0.66) !important;
          font-size: 12px !important;
          line-height: 1.55 !important;
        }

        [data-testid="stSidebar"] div.stButton > button,
        [data-testid="stSidebar"] div[data-testid="stDownloadButton"] button,
        [data-testid="stSidebar"] [data-testid="stFormSubmitButton"] button {
          width: 100% !important;
          min-height: 42px !important;
          padding: 0 14px !important;
          border: 1px solid rgba(15,107,87,0.12) !important;
          border-radius: 16px !important;
          background: rgba(255,255,255,0.46) !important;
          color: var(--cp-brand-deep) !important;
          box-shadow: none !important;
          font-size: 13px !important;
          font-weight: 760 !important;
        }

        [data-testid="stSidebar"] div.stButton > button:hover,
        [data-testid="stSidebar"] div[data-testid="stDownloadButton"] button:hover,
        [data-testid="stSidebar"] [data-testid="stFormSubmitButton"] button:hover {
          transform: none !important;
          border-color: rgba(15,107,87,0.22) !important;
          background: rgba(238,248,243,0.76) !important;
          box-shadow: 0 8px 20px rgba(18,45,36,0.05) !important;
        }

        [data-testid="stSidebar"] [data-testid="stTextInput"] input,
        [data-testid="stSidebar"] [data-testid="stNumberInput"] input,
        [data-testid="stSidebar"] div[data-baseweb="select"] > div {
          min-height: 42px !important;
          border-radius: 14px !important;
          font-size: 13px !important;
        }

        [data-testid="stSidebar"] [data-testid="stTextArea"] textarea {
          min-height: 128px !important;
          padding: 14px !important;
          border-radius: 16px !important;
          font-size: 13px !important;
        }

        /* 旧的 cp-sidebar-toolbox 不再作为卡片使用，避免底部工具区显得很重 */
        .cp-sidebar-toolbox {
          margin: 0 !important;
          padding: 0 !important;
          border: 0 !important;
          border-radius: 0 !important;
          background: transparent !important;
          box-shadow: none !important;
          backdrop-filter: none !important;
          -webkit-backdrop-filter: none !important;
        }

        /* ---------- main segmented nav cleanup ---------- */

        /* 不要让右侧功能区出现原生 radio 圆点 */
        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) input[type="radio"],
        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) svg,
        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label[data-baseweb="radio"] > div:first-child,
        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) [data-baseweb="radio"] > div:first-child {
          display: none !important;
          visibility: hidden !important;
          width: 0 !important;
          min-width: 0 !important;
          height: 0 !important;
          margin: 0 !important;
          padding: 0 !important;
          opacity: 0 !important;
        }

        /* 不要再额外画选中红点，解决大圈套小圈 */
        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label::before,
        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label:has(input:checked)::before {
          content: none !important;
          display: none !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) {
          display: flex !important;
          width: min(100%, 760px) !important;
          margin: 2px 0 34px !important;
          padding: 7px !important;
          border: 1px solid rgba(24,43,36,0.08) !important;
          border-radius: 999px !important;
          background: rgba(255,255,255,0.58) !important;
          box-shadow: 0 18px 46px rgba(18,45,36,0.07) !important;
          backdrop-filter: blur(22px) saturate(160%) !important;
          -webkit-backdrop-filter: blur(22px) saturate(160%) !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) div[role="radiogroup"] {
          display: flex !important;
          width: 100% !important;
          gap: 8px !important;
          align-items: center !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label,
        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label[data-baseweb="radio"] {
          position: relative !important;
          display: inline-flex !important;
          flex: 1 1 0 !important;
          align-items: center !important;
          justify-content: center !important;
          min-width: 0 !important;
          min-height: 52px !important;
          margin: 0 !important;
          padding: 0 22px !important;
          border: 1px solid transparent !important;
          border-radius: 999px !important;
          background: transparent !important;
          color: rgba(24,43,36,0.78) !important;
          box-shadow: none !important;
          font-size: 16px !important;
          font-weight: 760 !important;
          line-height: 1.2 !important;
          transition: background .18s ease, color .18s ease, box-shadow .18s ease, border-color .18s ease !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label p,
        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label span {
          margin: 0 !important;
          color: inherit !important;
          font-size: inherit !important;
          line-height: 1.2 !important;
          font-weight: inherit !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label:hover {
          background: rgba(238,248,243,0.62) !important;
          color: var(--cp-brand-deep) !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label:has(input:checked),
        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label[data-baseweb="radio"]:has(input:checked) {
          border-color: rgba(15,107,87,0.14) !important;
          background: rgba(238,248,243,0.98) !important;
          color: var(--cp-brand-deep) !important;
          box-shadow: inset 0 1px 0 rgba(255,255,255,0.92), 0 10px 28px rgba(18,45,36,0.08) !important;
        }

        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label:has(input:checked) *,
        [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label[data-baseweb="radio"]:has(input:checked) * {
          color: var(--cp-brand-deep) !important;
        }

        /* tabs 和 segmented nav 的视觉统一，但不使用圆形指示器 */
        div[data-baseweb="tab-list"],
        .stTabs [data-baseweb="tab-list"] {
          border-radius: 999px !important;
        }

        div[data-baseweb="tab"],
        .stTabs [data-baseweb="tab"] {
          border-radius: 999px !important;
        }

        /* ---------- content cards are lighter and less bulky ---------- */

        .cp-workspace-card,
        .cp-panel,
        .cp-result-panel,
        .cp-decision-card,
        .cp-empty-state,
        .cp-revision-card,
        .cp-job-card,
        .cp-note-card,
        .cp-score-card,
        .cp-score-pill,
        [data-testid="stVerticalBlockBorderWrapper"],
        [data-testid="stMetric"],
        [data-testid="stAlert"] {
          border-color: rgba(255,255,255,0.68) !important;
          background: rgba(255,255,255,0.58) !important;
          box-shadow: 0 16px 46px rgba(18,45,36,0.065) !important;
        }

        [data-testid="stVerticalBlockBorderWrapper"] > div {
          padding: 24px 26px !important;
        }

        .cp-panel-title {
          margin: 0 0 14px !important;
          color: var(--cp-text) !important;
          font-size: 20px !important;
          line-height: 1.25 !important;
          font-weight: 840 !important;
        }

        @media (max-width: 900px) {
          [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) {
            width: 100% !important;
          }

          [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) div[role="radiogroup"] {
            flex-direction: column !important;
          }

          [data-testid="stRadio"]:not([data-testid="stSidebar"] [data-testid="stRadio"]) label {
            width: 100% !important;
            min-height: 46px !important;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
