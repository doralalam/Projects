import React, { useEffect, useState, useRef } from "react";
import { fetchFundsList, fetchMomPivot, fetchDrilldown } from "../api/api";

function FundTable() {
  const [amcOptions, setAmcOptions] = useState([]);
  const [fundOptions, setFundOptions] = useState({});
  const [amc, setAmc] = useState("");
  const [fund, setFund] = useState("");
  const [data, setData] = useState([]);
  const [columns, setColumns] = useState([]);
  const [latestCol, setLatestCol] = useState("");
  const [sortConfig, setSortConfig] = useState({ column: null, direction: "desc", type: "weight" });
  const [scrollLeft, setScrollLeft] = useState(0);
  const [showMoM, setShowMoM] = useState(true);

  const scrollRef = useRef(null);

  useEffect(() => {
    fetchFundsList().then(res => {
      if (!res || typeof res !== "object") return;
      const amcs = Object.keys(res);
      setAmcOptions(amcs);
      setFundOptions(res);
      const firstAmc = amcs[0];
      setAmc(firstAmc);
      setFund(res[firstAmc][0]);
    });
  }, []);

  useEffect(() => {
    if (!amc || !fund) return;

    fetchMomPivot(amc, fund).then(res => {
      if (!Array.isArray(res) || res.length === 0) {
        setData([]);
        setColumns([]);
        return;
      }

      const dynamicCols = Object.keys(res[0]).filter(col => col !== "stock" && col !== "sector");
      const weightCols = dynamicCols.filter(col => col.includes("_weight"));
      const latestWeightCol = weightCols[0] || "";

      setLatestCol(latestWeightCol);
      setColumns(dynamicCols);

      const sorted = [...res].sort((a, b) => (b[latestWeightCol] ?? 0) - (a[latestWeightCol] ?? 0));
      setData(sorted);

      setSortConfig({ column: latestWeightCol, direction: "desc", type: "weight" });
    });
  }, [amc, fund]);

  useEffect(() => {
    const handleScroll = () => setScrollLeft(scrollRef.current.scrollLeft);
    scrollRef.current?.addEventListener("scroll", handleScroll);
    return () => scrollRef.current?.removeEventListener("scroll", handleScroll);
  }, []);

  const monthColumns = columns.filter(col => col.includes("_weight"));
  const prevCol = monthColumns[1];

  const latestMomValues = data.map(row =>
    latestCol ? row[`${latestCol.replace("_weight", "")}_mom`] ?? 0 : 0
  );

  const top5Positive = [...latestMomValues].filter(v => v > 0).sort((a, b) => b - a).slice(0, 5);
  const top5Negative = [...latestMomValues].filter(v => v < 0).sort((a, b) => a - b).slice(0, 5);

  const getLightBgColor = mom =>
    mom > 0 ? "#f0fff0" : mom < 0 ? "#fff0f0" : "#fff";

  const handleSort = (col, type = "weight") => {
    if (!showMoM && type === "mom") return;

    if (sortConfig.column === col && sortConfig.type === type) {
      setSortConfig({
        column: col,
        direction: sortConfig.direction === "asc" ? "desc" : "asc",
        type
      });
    } else {
      setSortConfig({ column: col, direction: "desc", type });
    }
  };

  const sortedData = [...data].sort((a, b) => {
    if (!sortConfig.column) return 0;

    let aVal = a[sortConfig.column];
    let bVal = b[sortConfig.column];

    if (sortConfig.type === "mom") {
      const base = sortConfig.column.replace("_weight", "");
      aVal = a[`${base}_mom`] ?? 0;
      bVal = b[`${base}_mom`] ?? 0;
    }

    return sortConfig.direction === "asc" ? aVal - bVal : bVal - aVal;
  });

  const fundsForSelectedAmc = fundOptions[amc] || [];

  return (
    <div style={{ padding: 20 }}>
      <h2>Monthly Portfolio Disclosures</h2>

      <select value={amc} onChange={e => setAmc(e.target.value)}>
        {amcOptions.map(a => <option key={a}>{a}</option>)}
      </select>

      <select value={fund} onChange={e => setFund(e.target.value)}>
        {fundsForSelectedAmc.map(f => <option key={f}>{f}</option>)}
      </select>

      <label>
        <input type="checkbox" checked={showMoM} onChange={e => setShowMoM(e.target.checked)} />
        Show MoM
      </label>

      <div ref={scrollRef} style={{ overflow: "auto", maxHeight: 400 }}>
        <table border="1">
          <thead>
            <tr>
              <th onClick={() => handleSort("stock")}>Stock</th>
              <th>Sector</th>
              {monthColumns.map(col => <th key={col}>{col}</th>)}
            </tr>
          </thead>

          <tbody>
            {sortedData.map((row, i) => {
              const latestVal = row[latestCol] ?? 0;
              const prevVal = prevCol ? row[prevCol] ?? 0 : 0;

              let tag = "";
              if (latestVal > 0 && prevVal === 0) tag = "Fresh";
              if (latestVal === 0 && prevVal > 0) tag = "Exit";

              return (
                <tr key={i}>
                  <td>
                    {row.stock} {tag && <span>{tag}</span>}
                  </td>
                  <td>{row.sector}</td>

                  {monthColumns.map(col => {
                    const base = col.replace("_weight", "");
                    const weight = row[col];
                    const mom = row[`${base}_mom`];

                    return (
                      <td style={{ background: showMoM ? getLightBgColor(mom) : "#fff" }}>
                        {weight ?? "-"} {showMoM && `(${mom ?? "-"})`}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default FundTable;