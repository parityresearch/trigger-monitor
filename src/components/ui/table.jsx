import React from 'react'

export function Table({ className = '', ...props }) {
  return <table className={`w-full text-sm ${className}`} {...props} />
}
export function TableHeader(props){ return <thead {...props} /> }
export function TableBody(props){ return <tbody {...props} /> }
export function TableRow({ className='', ...props}){ return <tr className={`border-b border-border hover:bg-muted/50 ${className}`} {...props} /> }
export function TableHead({ className='', ...props}){ return <th className={`px-4 py-3 text-left font-medium text-muted-foreground ${className}`} {...props} /> }
export function TableCell({ className='', ...props}){ return <td className={`px-4 py-3 align-top ${className}`} {...props} /> }
