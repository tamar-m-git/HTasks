// frontend/src/AppContent.js
import React, { useEffect, useState } from 'react';
// שימו לב: אין כאן ייבוא של BrowserRouter, הוא ב-App.js
import { Routes, Route, Link, useNavigate, Navigate } from 'react-router-dom';
import { jwtDecode } from 'jwt-decode';
import LoginPage from './components/LoginPage'; // נתיב יחסי נכון ל-AppContent.js
import RegisterPage from './components/RegisterPage'; // נתיב יחסי נכון ל-AppContent.js
import './styles/App.css'; // Main application styles

const AppContent = () => { // שים לב ששם הקומפוננטה הוא AppContent
  const [userRole, setUserRole] = useState(null);
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    const token = localStorage.getItem('token');
    if (token) {
      try {
        const decodedToken = jwtDecode(token);
        setUserRole(decodedToken.role);
        setIsLoggedIn(true);
        // Automatically navigate after successful login/token check
        // Uncomment and refine these navigations once dashboards are built
        // if (decodedToken.role === 'owner') {
        //   navigate('/owner-dashboard');
        // } else if (decodedToken.role === 'supplier') {
        //   navigate('/supplier-dashboard');
        // }
      } catch (error) {
        console.error('Invalid token or token expired:', error);
        localStorage.removeItem('token');
        localStorage.removeItem('role');
        setUserRole(null);
        setIsLoggedIn(false);
        navigate('/login'); // Redirect to login if token is invalid/expired
      }
    } else {
      setIsLoggedIn(false);
      setUserRole(null); // Ensure role is cleared if no token
    }
  }, [navigate]);

  const handleLogout = () => {
    localStorage.removeItem('token');
    localStorage.removeItem('role');
    setUserRole(null);
    setIsLoggedIn(false);
    navigate('/login'); // Redirect to login page after logout
    window.location.reload(); // Force a reload to clear all states and re-render App from scratch
  };

  return (
    // שימו לב: אין כאן את תגיות <Router> ו-</Router>!
    <div className="App">
      <nav className="navbar">
        <Link to="/" className="navbar-brand">Grocery Store Management</Link>
        <div className="navbar-links">
          {isLoggedIn ? (
            <>
              <span>Hello, {userRole === 'owner' ? 'Store Owner' : 'Supplier'}!</span>
              <button onClick={handleLogout} className="logout-button">Logout</button>
            </>
          ) : (
            <>
              <Link to="/login">Login</Link>
              <Link to="/register">Register</Link>
            </>
          )}
        </div>
      </nav>
      <div className="content">
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route path="/register" element={<RegisterPage />} />
          {/* Protected routes will go here once dashboards are ready */}
          {/* <Route
            path="/owner-dashboard"
            element={isLoggedIn && userRole === 'owner' ? <OwnerDashboard /> : <Navigate to="/login" />}
          /> */}
          {/* <Route
            path="/supplier-dashboard"
            element={isLoggedIn && userRole === 'supplier' ? <SupplierDashboard /> : <Navigate to="/login" />}
          /> */}
          <Route path="/" element={
            isLoggedIn ? (
              <div>
                <h1>Welcome to the Grocery Store Management System!</h1>
                <p>You are logged in as a {userRole === 'owner' ? 'Store Owner' : 'Supplier'}.</p>
                <p>Your respective dashboard will be accessible here later.</p>
              </div>
            ) : (
              <p>Please login or register to continue.</p>
            )
          } />
          {/* Catch-all route for any unhandled paths */}
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </div>
    </div>
  );
};

export default AppContent; // שים לב ששם הקומפוננטה הוא AppContent