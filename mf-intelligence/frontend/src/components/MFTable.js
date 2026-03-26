import React, { useEffect, useState } from "react";
import { fetchMFTable, fetchDrilldown } from "../api/api";

function MFTable({ type, title }) {
  const [data, setData] = useState([]);
  const [expanded, setExpanded] = useState({});
  const [details, setDetails] = useState({});

  useEffect(() => {
    fetchMFTable(type).then(res => setData(res));
  }, [type]);

  const toggle = async (isin) => {
    if (!details[isin]) {
      const res = await fetchDrilldown(isin);
      setDetails(prev => ({ ...prev, [isin]: res }));
    }
    setExpanded(prev => ({ ...prev, [isin]: !prev[isin] }));
  };

  return (
    <div style={{ marginBottom: 20 }}>
      <h3>{title}</h3>
      <table border="1" width="100%" cellPadding="5" style={{ borderCollapse: "collapse" }}>
        <thead style={{ backgroundColor: "#f2f2f2" }}>
          <tr>
            <th>Stock</th>
            <th>Change</th>
            <th>Type</th>
            <th>Details</th>
          </tr>
        </thead>
        <tbody>
          {data.map(row => (
            <React.Fragment key={row.isin}>
              <tr>
                <td>{row.stock}</td>
                <td>{row.change}</td>
                <td>{row.bucket}</td>
                <td>
                  <button onClick={() => toggle(row.isin)}>
                    {expanded[row.isin] ? "Hide" : "Show"} Details
                  </button>
                </td>
              </tr>

              {expanded[row.isin] && (
                <tr>
                  <td colSpan="4" style={{ backgroundColor: "#f9fafb", paddingLeft: 20 }}>
                    {(details[row.isin] || []).map((d, i) => (
                      <div key={i} style={{ marginBottom: 4 }}>
                        {d.fund} ({d.amc}) - {d.change} ({d.bucket})
                      </div>
                    ))}
                  </td>
                </tr>
              )}
            </React.Fragment>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default MFTable;