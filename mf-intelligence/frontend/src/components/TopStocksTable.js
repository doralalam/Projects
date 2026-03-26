import React, { useEffect, useState } from "react";
import { fetchMFTable } from "../api/api";

const BASE_URL = "http://127.0.0.1:8000";

function TopStocksTable({ type, title }) {
  const [data, setData] = useState([]);
  const [limit, setLimit] = useState(10);
  const [expanded, setExpanded] = useState({});
  const [details, setDetails] = useState({});

  useEffect(() => {
    const fetchAndSort = async () => {
      let res = await fetchMFTable(type, 1000);

      const lowerType = type.toLowerCase();
      if (["exit", "decreased"].includes(lowerType)) {
        res.sort((a, b) => (a.change ?? 0) - (b.change ?? 0));
      } else {
        res.sort((a, b) => (b.change ?? 0) - (a.change ?? 0));
      }

      setData(res.slice(0, limit));
    };

    fetchAndSort();
  }, [type, limit]);

  const toggleRow = async (isin) => {
    if (expanded[isin]) {
      setExpanded(prev => ({ ...prev, [isin]: false }));
      return;
    }

    if (!details[isin]) {
      const res = await fetch(`${BASE_URL}/mf/drilldown?isin=${isin}`);
      const json = await res.json();
      setDetails(prev => ({ ...prev, [isin]: json }));
    }

    setExpanded(prev => ({ ...prev, [isin]: true }));
  };

  const getColor = (bucket) => {
    if (["Fresh", "Increased"].includes(bucket)) return "green";
    if (["Exit", "Decreased"].includes(bucket)) return "red";
    return "black";
  };

  return (
    <div className="card" style={{ marginBottom: 20 }}>
      <h4>{title}</h4>

      <table className="table" border="1" cellPadding="5" style={{ borderCollapse: "collapse", width: "100%" }}>
        <thead style={{ backgroundColor: "#f2f2f2" }}>
          <tr>
            <th></th>
            <th>Stock</th>
            <th>Sector</th>
            <th style={{ textAlign: "center" }}>Change</th>
            <th style={{ textAlign: "center" }}>Funds</th>
            <th>Bucket</th>
          </tr>
        </thead>

        <tbody>
          {data.map((row, i) => (
            <React.Fragment key={i}>
              <tr onClick={() => toggleRow(row.isin)} style={{ cursor: "pointer" }}>
                <td>{expanded[row.isin] ? "▼" : "▶"}</td>
                <td>{row.stock}</td>
                <td>{row.sector}</td>
                <td style={{ textAlign: "center", color: row.change > 0 ? "green" : row.change < 0 ? "red" : "black" }}>
                  {row.change > 0 ? "+" : ""}{(row.change ?? 0).toFixed(2)}
                </td>
                <td style={{ textAlign: "center" }}>{row.fund_count}</td>
                <td style={{ color: getColor(row.bucket) }}>{row.bucket}</td>
              </tr>

              {expanded[row.isin] && (
                <tr>
                  <td colSpan="6" style={{ background: "#f9fafb", paddingLeft: 20 }}>
                    <table style={{ width: "100%" }} border="1" cellPadding="3">
                      <thead style={{ backgroundColor: "#eaeaea" }}>
                        <tr>
                          <th>AMC</th>
                          <th>Fund</th>
                          <th style={{ textAlign: "center" }}>Change</th>
                          <th style={{ textAlign: "center" }}>Bucket</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(details[row.isin] || []).map((d, j) => (
                          <tr key={j}>
                            <td>{d.amc}</td>
                            <td>{d.fund}</td>
                            <td style={{ textAlign: "center", color: d.change > 0 ? "green" : d.change < 0 ? "red" : "black" }}>
                              {d.change > 0 ? "+" : ""}{(d.change ?? 0).toFixed(2)}
                            </td>
                            <td style={{ textAlign: "center", color: getColor(d.bucket) }}>{d.bucket}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </td>
                </tr>
              )}
            </React.Fragment>
          ))}
        </tbody>
      </table>

      <div className="expand-btn" style={{ marginTop: 10 }}>
        <span onClick={() => setLimit(10)} style={{ cursor: "pointer" }}>Top 10</span> |{" "}
        <span onClick={() => setLimit(100)} style={{ cursor: "pointer" }}>Top 100</span> |{" "}
        <span onClick={() => setLimit(1000)} style={{ cursor: "pointer" }}>All</span>
      </div>
    </div>
  );
}

export default TopStocksTable;