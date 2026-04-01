import React from "react";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import Layout from "./components/Layout.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import Breaks from "./pages/Breaks.jsx";
import JiraPage from "./pages/Jira.jsx";
import ThemesPage from "./pages/Themes.jsx";
import RecsPage from "./pages/Recs.jsx";

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="dashboard" element={<Dashboard />} />
          <Route path="breaks"    element={<Breaks />} />
          <Route path="recs"      element={<RecsPage />} />
          <Route path="jira"      element={<JiraPage />} />
          <Route path="themes"    element={<ThemesPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
