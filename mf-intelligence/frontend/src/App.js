import React, { useEffect, useState, useRef } from "react";

function App() {
  const [data, setData] = useState([]);
  const [columns, setColumns] = useState([]);
  const [latestCol, setLatestCol] = useState("");
  const [sortConfig, setSortConfig] = useState({
    column: null,
    direction: "desc",
    type: "weight",
  });
  const [scrollLeft, setScrollLeft] = useState(0);
  const scrollRef = useRef(null);

  useEffect(() => {
    fetch("http://127.0.0.1:8000/fund/HDFC%20Flexi%20Cap%20Fund/mom-pivot")
      .then((res) => res.json())
      .then((res) => {
        if (!Array.isArray(res) || res.length === 0) return;

        const dynamicCols = Object.keys(res[0]).filter(
          (col) => col !== "stock" && col !== "sector"
        );
        const weightCols = dynamicCols.filter((col) => col.includes("_weight"));
        const latestWeightCol = weightCols[0];
        setLatestCol(latestWeightCol);

        const sortedDataInitial = [...res].sort((a, b) => {
          const valA = a[latestWeightCol] ?? 0;
          const valB = b[latestWeightCol] ?? 0;
          return valB - valA;
        });

        setColumns(dynamicCols);
        setData(sortedDataInitial);
      });
  }, []);

  // Track horizontal scroll
  useEffect(() => {
    const handleScroll = () => setScrollLeft(scrollRef.current.scrollLeft);
    const container = scrollRef.current;
    container.addEventListener("scroll", handleScroll);
    return () => container.removeEventListener("scroll", handleScroll);
  }, []);

  const monthColumns = columns.filter((col) => col.includes("_weight"));
  const prevCol = monthColumns[1];

  const latestMomValues = data.map(
    (row) => row[`${latestCol.replace("_weight", "")}_mom`] ?? 0
  );
  const top5Positive = [...latestMomValues].filter((v) => v > 0).sort((a, b) => b - a).slice(0, 5);
  const top5Negative = [...latestMomValues].filter((v) => v < 0).sort((a, b) => a - b).slice(0, 5);

  const getLightBgColor = (mom) => (mom > 0 ? "#f0fff0" : mom < 0 ? "#fff0f0" : "#fff");

  const handleSort = (col, type = "weight") => {
    if (sortConfig.column === col && sortConfig.type === type) {
      setSortConfig({
        column: col,
        direction: sortConfig.direction === "asc" ? "desc" : "asc",
        type,
      });
    } else {
      setSortConfig({ column: col, direction: "desc", type });
    }
  };

  const sortedData = [...data].sort((a, b) => {
    if (!sortConfig.column) return 0;
    let aVal = a[sortConfig.column];
    let bVal = b[sortConfig.column];
    if (sortConfig.column.includes("_weight") && sortConfig.type === "mom") {
      const base = sortConfig.column.replace("_weight", "");
      aVal = a[`${base}_mom`] ?? 0;
      bVal = b[`${base}_mom`] ?? 0;
    }
    aVal = aVal ?? 0;
    bVal = bVal ?? 0;
    return aVal < bVal ? (sortConfig.direction === "asc" ? -1 : 1) : aVal > bVal ? (sortConfig.direction === "asc" ? 1 : -1) : 0;
  });

  return (
    <div style={{ padding: 20, height: "100vh", boxSizing: "border-box" }}>
      <h2>MoM Portfolio Changes</h2>
      <div ref={scrollRef} style={{ overflowX: "auto", overflowY: "auto", maxHeight: "calc(100vh - 80px)", border: "1px solid #ccc" }}>
        <table style={{ borderCollapse: "collapse", tableLayout: "auto", minWidth: "100%", fontSize: "13px" }}>
          <thead style={{ backgroundColor: "#f2f2f2", position: "sticky", top: 0, zIndex: 3, fontWeight: 700 }}>
            <tr>
              {/* Stock sticky */}
              <th style={{ position: "sticky", left: 0, zIndex: 5, backgroundColor: "#f2f2f2", padding: "6px 12px", borderRight: "2px solid #ccc", minWidth: 150, cursor: "pointer" }} onClick={() => handleSort("stock")}>
                Stock {sortConfig.column === "stock" ? (sortConfig.direction === "asc" ? " ↑" : " ↓") : ""}
              </th>

              {/* Sector sticky, collapses behind Stock */}
              <th
                style={{
                  position: "sticky",
                  left: 0, // always start at left=0 (behind stock)
                  zIndex: 4,
                  backgroundColor: "#f2f2f2",
                  padding: "6px 8px",
                  borderRight: "2px solid #ccc",
                  width: 60,
                  minWidth: 60,
                  cursor: "pointer",
                  transform: `translateX(${scrollLeft > 0 ? -Math.min(scrollLeft, 60) : 0}px)`,
                  transition: "transform 0.2s",
                }}
                onClick={() => handleSort("sector")}
              >
                Sector {sortConfig.column === "sector" ? (sortConfig.direction === "asc" ? " ↑" : " ↓") : ""}
              </th>

              {/* Monthly headers */}
              {monthColumns.map((col, idx) => {
                const baseName = col.replace("_weight", "");
                const isSortedMoM = sortConfig.column === col && sortConfig.type === "mom";
                const isSortedWeight = sortConfig.column === col && sortConfig.type === "weight";
                return (
                  <th key={idx} style={{ minWidth: 120, textAlign: "center", fontSize: 13, padding: "6px 0" }}>
                    <div style={{ display: "flex", justifyContent: "center", alignItems: "center" }}>
                      <span title="Sort by MoM" onClick={() => handleSort(col, "mom")} style={{ cursor: "pointer", marginRight: 4 }}>
                        {isSortedMoM ? (sortConfig.direction === "asc" ? "▲" : "▼") : "△"}
                      </span>
                      <span style={{ fontWeight: 600 }}>{baseName}</span>
                      <span title="Sort by Weight" onClick={() => handleSort(col, "weight")} style={{ cursor: "pointer", marginLeft: 4 }}>
                        {isSortedWeight ? (sortConfig.direction === "asc" ? "▲" : "▼") : "△"}
                      </span>
                    </div>
                  </th>
                );
              })}
            </tr>
          </thead>

          <tbody>
            {sortedData.map((row, i) => {
              const latestMom = row[`${latestCol.replace("_weight", "")}_mom`] ?? 0;
              const latestValue = row[latestCol] ?? 0;
              const prevValue = prevCol ? row[prevCol] ?? 0 : 0;
              let freshExit = "";
              if (latestValue > 0 && (prevValue === 0 || prevValue === null)) freshExit = "Fresh";
              else if ((latestValue === 0 || latestValue === null) && prevValue > 0) freshExit = "Exit";

              const isTopPositive = top5Positive.includes(latestMom);
              const isTopNegative = top5Negative.includes(latestMom);

              return (
                <tr key={i} style={{ borderBottom: "1px solid #e0e0e0" }}>
                  {/* Stock */}
                  <td style={{ position: "sticky", left: 0, backgroundColor: "#fff", padding: "4px 12px", whiteSpace: "nowrap", fontWeight: 700, borderRight: "2px solid #ccc", zIndex: 5 }}>
                    {row.stock}
                    {(isTopPositive || isTopNegative) && (
                      <span style={{ marginLeft: 8, fontSize: 11, fontWeight: 600, color: isTopPositive ? "green" : "red", backgroundColor: isTopPositive ? "#e6f9e6" : "#ffe6e6", padding: "2px 4px", borderRadius: 4 }}>
                        {isTopPositive ? "Increased" : "Decreased"}
                      </span>
                    )}
                    {freshExit && (
                      <span style={{ marginLeft: 4, fontSize: 11, fontWeight: 600, color: freshExit === "Fresh" ? "green" : "red", backgroundColor: freshExit === "Fresh" ? "#f0fff0" : "#fff0f0", padding: "2px 4px", borderRadius: 4 }}>
                        {freshExit}
                      </span>
                    )}
                  </td>

                  {/* Sector collapses behind Stock */}
                  <td
                    style={{
                      position: "sticky",
                      left: 0,
                      zIndex: 4,
                      backgroundColor: "#fff",
                      fontSize: 12,
                      padding: "4px 8px",
                      width: 60,
                      whiteSpace: "nowrap",
                      borderRight: "2px solid #ccc",
                      transform: `translateX(${scrollLeft > 0 ? -Math.min(scrollLeft, 60) : 0}px)`,
                      transition: "transform 0.2s",
                    }}
                  >
                    {row.sector}
                  </td>

                  {/* Monthly cells */}
                  {monthColumns.map((col, j) => {
                    const base = col.replace("_weight", "");
                    const weight = row[col];
                    const mom = row[`${base}_mom`];
                    let momColor = mom > 0 ? "green" : mom < 0 ? "red" : "inherit";
                    return (
                      <td key={j} style={{ textAlign: "center", whiteSpace: "nowrap", fontSize: 13, padding: "4px 12px", backgroundColor: getLightBgColor(mom) }}>
                        <div style={{ fontWeight: 600 }}>{weight !== null ? weight : "-"}</div>
                        <div style={{ color: momColor, fontSize: 11 }}>{mom !== null ? (mom > 0 ? "+" : "") + mom + "%" : "-"}</div>
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

export default App;