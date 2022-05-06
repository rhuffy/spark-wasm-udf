import React from "react";
import Files from "./Files";
import Home from "./Home";

import { Routes, Route, useNavigate } from "react-router-dom";
import { Alignment, Button, Navbar } from "@blueprintjs/core";

import "./App.css";

function App() {
  let navigate = useNavigate();

  return (
    <div className="App">
      <Navbar>
        <Navbar.Group align={Alignment.LEFT}>
          <Navbar.Heading>Spark Wasm UDF</Navbar.Heading>
          <Navbar.Divider />
          <Button
            className="bp4-minimal"
            icon="home"
            text="Home"
            onClick={() => navigate("/")}
          />
          <Button
            className="bp4-minimal"
            icon="document"
            text="Files"
            onClick={() => navigate("files")}
          />
        </Navbar.Group>
      </Navbar>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="files" element={<Files />} />
      </Routes>
    </div>
  );
}

export default App;
