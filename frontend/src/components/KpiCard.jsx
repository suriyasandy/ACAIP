import React from 'react'
const VARIANTS = {
  blue:'bg-blue-50 border-blue-200 text-blue-700',
  green:'bg-green-50 border-green-200 text-green-700',
  red:'bg-red-50 border-red-200 text-red-700',
  amber:'bg-amber-50 border-amber-200 text-amber-700',
  purple:'bg-purple-50 border-purple-200 text-purple-700',
}
export default function KpiCard({ title, value, subtitle, variant='blue', icon: Icon }) {
  const cls = VARIANTS[variant] || VARIANTS.blue
  return (
    <div className={`rounded-lg border p-4 ${cls} flex flex-col gap-1`}>
      <div className="flex items-center justify-between">
        <span className="text-xs font-semibold uppercase tracking-wide opacity-70">{title}</span>
        {Icon && <Icon className="w-4 h-4 opacity-50" />}
      </div>
      <div className="text-2xl font-bold">{value ?? '—'}</div>
      {subtitle && <div className="text-xs opacity-60">{subtitle}</div>}
    </div>
  )
}
