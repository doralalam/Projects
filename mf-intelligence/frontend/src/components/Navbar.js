import React from "react";

function Navbar({ page, setPage }) {
  const pages = [
    { key: "home", label: "Home" },
    { key: "monthly", label: "Monthly Disclosures" }
  ];

  return (
    <div className="navbar" style={{ display: "flex", gap: 20, padding: 10 }}>
      {pages.map(p => (
        <div
          key={p.key}
          className={`nav-item ${page === p.key ? "active" : ""}`}
          onClick={() => setPage(p.key)}
          style={{
            cursor: "pointer",
            fontWeight: page === p.key ? "bold" : "normal",
            borderBottom: page === p.key ? "2px solid #000" : "none",
            padding: "5px 10px"
          }}
        >
          {p.label}
        </div>
      ))}
    </div>
  );
}

export default Navbar;