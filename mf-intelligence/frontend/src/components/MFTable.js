import React, { useEffect, useState } from "react";
import { fetchMFTable, fetchDrilldown } from "../api/api";

function MFTable({ type, title }) {
  const [data, setData] = useState([]);
  const [expanded, setExpanded] = useState({});
  const [details, setDetails] = useState({});

  useEffect(() => {
    fetchMFTable(type).then(setData);
  }, [type]);

  const toggle = async (isin) => {
    if (!details[isin]) {
      const res = await fetchDrilldown(isin);
      setDetails(prev => ({ ...prev, [isin]: res }));
    }
    setExpanded(prev => ({ ...prev, [isin]: !prev[isin] }));
  };

  return (
    <div>
      <h3>{title}</h3>

      <table border="1" width="100%">
        <tbody>
          {data.map(row => (
            <React.Fragment key={row.isin}>
              <tr>
                <td>{row.stock}</td>
                <td>{row.total_diff}</td>
                <td><button onClick={() => toggle(row.isin)}>Toggle</button></td>
              </tr>

              {expanded[row.isin] && (
                <tr>
                  <td colSpan="3">
                    {(details[row.isin] || []).map((d, i) => (
                      <div key={i}>{d.fund} - {d.diff}</div>
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