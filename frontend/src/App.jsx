import React from "react";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import Layout from "./components/Layout.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import Upload from "./pages/Upload.jsx";
import ValidationReport from "./pages/ValidationReport.jsx";
import ErrorBoundary from "./components/ErrorBoundary.jsx";

function Guarded({ children }) {
  return <ErrorBoundary>{children}</ErrorBoundary>;
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="dashboard"  element={<Guarded><Dashboard /></Guarded>} />
          <Route path="upload"     element={<Guarded><Upload /></Guarded>} />
          <Route path="validation" element={<Guarded><ValidationReport /></Guarded>} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
