import React from "react";

export default class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error) {
    return { error };
  }

  componentDidCatch(error, info) {
    console.error("Dashboard render error:", error, info);
  }

  render() {
    if (this.state.error) {
      return (
        <div style={{
          background: "rgba(244,63,94,0.08)", border: "1px solid rgba(244,63,94,0.3)",
          borderRadius: 10, padding: "24px 28px", color: "#fca5a5",
        }}>
          <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 8 }}>
            Page render error
          </div>
          <div style={{ fontSize: 12, color: "#f87171", fontFamily: "monospace", wordBreak: "break-all" }}>
            {this.state.error.message}
          </div>
          <button
            style={{
              marginTop: 16, background: "#334155", border: "none", borderRadius: 6,
              color: "#e2e8f0", padding: "7px 16px", fontSize: 12, cursor: "pointer",
            }}
            onClick={() => this.setState({ error: null })}
          >
            Retry
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}
