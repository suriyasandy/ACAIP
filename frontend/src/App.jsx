import React from "react";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import Layout from "./components/Layout.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import Breaks from "./pages/Breaks.jsx";
import JiraPage from "./pages/Jira.jsx";
import ThemesPage from "./pages/Themes.jsx";
import RecsPage from "./pages/Recs.jsx";
import Upload from "./pages/Upload.jsx";
import ModelTraining from "./pages/ModelTraining.jsx";
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
          <Route path="dashboard" element={<Guarded><Dashboard /></Guarded>} />
          <Route path="breaks"    element={<Guarded><Breaks /></Guarded>} />
          <Route path="recs"      element={<Guarded><RecsPage /></Guarded>} />
          <Route path="jira"      element={<Guarded><JiraPage /></Guarded>} />
          <Route path="themes"    element={<Guarded><ThemesPage /></Guarded>} />
          <Route path="upload"    element={<Guarded><Upload /></Guarded>} />
          <Route path="training"  element={<Guarded><ModelTraining /></Guarded>} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
