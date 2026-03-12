import React from 'react'

export function Separator({ className = '', ...props }) {
  return <div className={`h-px w-full bg-[hsl(var(--border))] ${className}`} {...props} />
}
