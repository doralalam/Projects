import React from "react";

function Navbar({ page, setPage }) {
  return (
    <div className="navbar">
      <div
        className={`nav-item ${page === "home" ? "active" : ""}`}
        onClick={() => setPage("home")}
      >
        Home
      </div>

      <div
        className={`nav-item ${page === "monthly" ? "active" : ""}`}
        onClick={() => setPage("monthly")}
      >
        Monthly Disclosures
      </div>
    </div>
  );
}

export default Navbar;