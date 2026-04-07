import { Routes, Route } from 'react-router'

function DashboardPage() {
  return <h1>Dashboard</h1>
}

function ProfilePage() {
  return <h1>Profile</h1>
}

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<DashboardPage />} />
      <Route path="/profile/:username" element={<ProfilePage />} />
    </Routes>
  )
}