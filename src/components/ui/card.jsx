import React from 'react'

export function Card({ className = '', ...props }) {
  return <div className={`border border-border bg-background shadow-sm ${className}`} {...props} />
}

export function CardHeader({ className = '', ...props }) {
  return <div className={`p-6 ${className}`} {...props} />
}

export function CardTitle({ className = '', ...props }) {
  return <div className={`font-semibold leading-none tracking-tight ${className}`} {...props} />
}

export function CardDescription({ className = '', ...props }) {
  return <div className={`text-sm text-muted-foreground ${className}`} {...props} />
}

export function CardContent({ className = '', ...props }) {
  return <div className={`p-6 pt-0 ${className}`} {...props} />
}
