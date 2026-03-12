import React from 'react'

const base = [
  'inline-flex items-center justify-center whitespace-nowrap text-sm font-medium',
  'transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-2',
  'disabled:pointer-events-none disabled:opacity-50',
].join(' ')

const variants = {
  default: 'bg-black text-white hover:opacity-90',
  secondary: 'bg-muted text-foreground hover:opacity-90',
  ghost: 'bg-transparent hover:bg-muted',
}

export function Button({ variant = 'default', className = '', href, target, rel, disabled, ...props }) {
  const classes = `${base} ${variants[variant] || variants.default} px-4 py-2 ${className}`
  if (href) {
    const safeRel = target === '_blank' ? rel || 'noreferrer' : rel
    return (
      <a
        href={href}
        target={target}
        rel={safeRel}
        className={classes}
        {...props}
      />
    )
  }
  return (
    <button
      className={classes}
      disabled={disabled}
      {...props}
    />
  )
}
