import React from 'react'

const base = 'inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-medium'

const variants = {
  outline: 'border-border text-foreground',
  secondary: 'border-transparent bg-[#f6d465] text-[#3b2f00]',
  destructive: 'border-transparent bg-[hsl(var(--destructive))] text-white',
}

export function Badge({ variant = 'outline', className = '', ...props }) {
  return <span className={`${base} ${variants[variant] || variants.outline} ${className}`} {...props} />
}
