import React from 'react'
const PLATFORMS = ['TLM','DUCO','D3S-CEQ','D3S-LND','D3S-MTN','D3S-OTC']
const ASSET_CLASSES = ['Cash Equities','Loans & Deposits','Medium Term Notes','OTC Derivatives']
export default function FilterBar({ filters, onChange }) {
  const toggle = (key, val) => {
    const current = filters[key] || []
    const next = current.includes(val) ? current.filter(v => v !== val) : [...current, val]
    onChange({ ...filters, [key]: next })
  }
  return (
    <div className="flex flex-wrap gap-4 bg-white border border-gray-200 rounded-lg p-3 mb-5 text-sm">
      <div>
        <span className="font-semibold text-gray-500 mr-2 text-xs uppercase">Platform:</span>
        {PLATFORMS.map(p => (
          <button key={p} onClick={() => toggle('platform', p)}
            className={`mr-1 px-2 py-0.5 rounded border text-xs transition-colors ${
              (filters.platform||[]).includes(p) ? 'bg-blue-600 border-blue-600 text-white' : 'border-gray-300 text-gray-600 hover:bg-gray-100'}`}>{p}</button>
        ))}
      </div>
      <div>
        <span className="font-semibold text-gray-500 mr-2 text-xs uppercase">Asset Class:</span>
        {ASSET_CLASSES.map(ac => (
          <button key={ac} onClick={() => toggle('asset_class', ac)}
            className={`mr-1 px-2 py-0.5 rounded border text-xs transition-colors ${
              (filters.asset_class||[]).includes(ac) ? 'bg-purple-600 border-purple-600 text-white' : 'border-gray-300 text-gray-600 hover:bg-gray-100'}`}>{ac}</button>
        ))}
      </div>
      <div className="flex items-center gap-2">
        <span className="font-semibold text-gray-500 text-xs uppercase">From:</span>
        <input type="date" value={filters.date_from||''} onChange={e => onChange({...filters,date_from:e.target.value})} className="border border-gray-300 rounded px-2 py-0.5 text-xs" />
        <span className="font-semibold text-gray-500 text-xs uppercase">To:</span>
        <input type="date" value={filters.date_to||''} onChange={e => onChange({...filters,date_to:e.target.value})} className="border border-gray-300 rounded px-2 py-0.5 text-xs" />
      </div>
      <button onClick={() => onChange({})} className="ml-auto text-xs text-gray-400 hover:text-red-500">Clear filters</button>
    </div>
  )
}
