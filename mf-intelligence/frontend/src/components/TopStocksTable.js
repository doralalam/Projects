import React, { useEffect, useState } from "react";
import { fetchMFTable } from "../api/api";

const BASE_URL = "http://127.0.0.1:8000";

function TopStocksTable({ type, title }) {
  const [data, setData] = useState([]);
  const [limit, setLimit] = useState(10);
  const [expanded, setExpanded] = useState({});
  const [details, setDetails] = useState({});

  useEffect(() => {
    fetchMFTable(type, limit).then(setData);
  }, [type, limit]);

  const toggleRow = async (isin) => {
    if (expanded[isin]) {
      setExpanded(prev => ({ ...prev, [isin]: false }));
      return;
    }

    if (!details[isin]) {
      const res = await fetch(`${BASE_URL}/mf/drilldown?isin=${isin}`);
      const json = await res.json();

      setDetails(prev => ({
        ...prev,
        [isin]: json
      }));
    }

    setExpanded(prev => ({
      ...prev,
      [isin]: true
    }));
  };

  const getColor = (type) => {
    if (type === "fresh" || type === "increased") return "green";
    if (type === "exit" || type === "decreased") return "red";
    return "black";
  };

  return (
    <div className="card">
      <h4>{title}</h4>

      <table className="table">
        <thead>
          <tr>
            <th></th>
            <th>Stock</th>
            <th>Sector</th>
            <th style={{ textAlign: "center" }}>Net Change</th>
            <th style={{ textAlign: "center" }}>Funds</th>
          </tr>
        </thead>

        <tbody>
          {data.map((row, i) => (
            <React.Fragment key={i}>
              <tr onClick={() => toggleRow(row.isin)} style={{ cursor: "pointer" }}>
                <td>{expanded[row.isin] ? "▼" : "▶"}</td>
                <td>{row.stock}</td>
                <td>{row.sector}</td>

                <td
                  style={{ textAlign: "center" }}
                  className={row.total_diff > 0 ? "green" : "red"}
                >
                  {row.total_diff > 0 ? "+" : ""}
                  {row.total_diff.toFixed(2)}
                </td>

                <td style={{ textAlign: "center" }}>
                  {row.fund_count}
                </td>
              </tr>

              {expanded[row.isin] && (
                <tr>
                  <td colSpan="5">
                    <table style={{ width: "100%", background: "#f9fafb" }}>
                      <thead>
                        <tr>
                          <th>AMC</th>
                          <th>Fund</th>
                          <th style={{ textAlign: "center" }}>Change</th>
                          <th style={{ textAlign: "center" }}>Type</th>
                        </tr>
                      </thead>

                      <tbody>
                        {(details[row.isin] || []).map((d, j) => (
                          <tr key={j}>
                            <td>{d.amc}</td>
                            <td>{d.fund}</td>

                            <td
                              style={{ textAlign: "center" }}
                              className={d.diff > 0 ? "green" : "red"}
                            >
                              {d.diff > 0 ? "+" : ""}
                              {d.diff.toFixed(2)}
                            </td>

                            <td
                              style={{
                                textAlign: "center",
                                color: getColor(d.movement_type)
                              }}
                            >
                              {d.movement_type.charAt(0).toUpperCase() + d.movement_type.slice(1)}
                            </td>
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

      <div className="expand-btn">
        <span onClick={() => setLimit(10)}>Top 10</span> |{" "}
        <span onClick={() => setLimit(100)}>Top 100</span> |{" "}
        <span onClick={() => setLimit(1000)}>All</span>
      </div>
    </div>
  );
}

export default TopStocksTable;