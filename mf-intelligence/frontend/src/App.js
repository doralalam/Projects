import React, { useState } from "react";
import "./App.css";
import Navbar from "./components/Navbar";
import Home from "./pages/Home";
import MonthlyDisclosure from "./pages/MonthlyDisclosure";

function App() {
  const [page, setPage] = useState("home");

  return (
    <div>
      <Navbar page={page} setPage={setPage} />

      {page === "home" && <Home />}
      {page === "monthly" && <MonthlyDisclosure />}
    </div>
  );
}

export default App;