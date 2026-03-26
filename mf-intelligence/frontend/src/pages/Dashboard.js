import React, { useState } from "react";
import FundTable from "../components/FundTable";
import MFTable from "../components/MFTable";

function Dashboard() {
  const [activeTab, setActiveTab] = useState("funds");
  const tabs = [
    { key: "funds", label: "Fund Table" },
    { key: "increased", label: "Increased" },
    { key: "decreased", label: "Decreased" },
    { key: "fresh", label: "Fresh" },
    { key: "exit", label: "Exit" }
  ];

  return (
    <div style={{ padding: 20 }}>
      <div style={{ display: "flex", gap: 20, marginBottom: 20 }}>
        {tabs.map(tab => (
          <div
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            style={{
              cursor: "pointer",
              fontWeight: activeTab === tab.key ? "bold" : "normal",
              borderBottom: activeTab === tab.key ? "2px solid #000" : "none",
              padding: "5px 10px"
            }}
          >
            {tab.label}
          </div>
        ))}
      </div>

      <div>
        {activeTab === "funds" && <FundTable />}
        {activeTab === "increased" && <MFTable type="increased" title="Increased" />}
        {activeTab === "decreased" && <MFTable type="decreased" title="Decreased" />}
        {activeTab === "fresh" && <MFTable type="fresh" title="Fresh" />}
        {activeTab === "exit" && <MFTable type="exit" title="Exit" />}
      </div>
    </div>
  );
}

export default Dashboard;