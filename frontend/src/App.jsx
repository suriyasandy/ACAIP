import React from 'react'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import PlatformSummary from './pages/PlatformSummary'
import AssetClassBreakdown from './pages/AssetClassBreakdown'
import RecHeatmap from './pages/RecHeatmap'
import ThemeAnalysis from './pages/ThemeAnalysis'
import JiraCoverage from './pages/JiraCoverage'
import BSCertification from './pages/BSCertification'
import UploadPage from './pages/UploadPage'
export default function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/" element={<PlatformSummary />} />
          <Route path="/asset-classes" element={<AssetClassBreakdown />} />
          <Route path="/recs" element={<RecHeatmap />} />
          <Route path="/themes" element={<ThemeAnalysis />} />
          <Route path="/jira" element={<JiraCoverage />} />
          <Route path="/certification" element={<BSCertification />} />
          <Route path="/upload" element={<UploadPage />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  )
}
