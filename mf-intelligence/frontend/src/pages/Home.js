import React from "react";
import TopStocksTable from "../components/TopStocksTable";

function Home() {
  return (
    <div className="container">
      <div className="grid">

        <TopStocksTable
          type="fresh"
          title="Top 10 Fresh Entries"
        />

        <TopStocksTable
          type="exit"
          title="Top 10 Exits"
        />

        <TopStocksTable
          type="increased"
          title="Top 10 Increased Holdings"
        />

        <TopStocksTable
          type="decreased"
          title="Top 10 Decreased Holdings"
        />

      </div>
    </div>
  );
}

export default Home;