import React from "react";
import FundTable from "../components/FundTable";
import MFTable from "../components/MFTable";

function Dashboard() {
  return (
    <div>
      <FundTable />
      <MFTable type="increased" title="Increased" />
      <MFTable type="decreased" title="Decreased" />
      <MFTable type="fresh" title="Fresh" />
      <MFTable type="exit" title="Exit" />
    </div>
  );
}

export default Dashboard;