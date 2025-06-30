// frontend/src/components/LoginPage.js
import React, { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import api from '../api';
import { jwtDecode } from 'jwt-decode';
import LoginForm from './auth/LoginForm'; // <--- תיקון נתיב כאן
import '../styles/Auth.css'; // Styles for authentication pages

const LoginPage = () => {
  const [error, setError] = useState(null);
  const navigate = useNavigate();

  const handleLoginSubmit = async (userName, password) => {
    setError(null);

    try {
      const response = await api.post('/users/login', { userName, password });
      const { token } = response.data;

      if (token) {
        localStorage.setItem('token', token);
        const decodedToken = jwtDecode(token);
        localStorage.setItem('role', decodedToken.role);

        if (decodedToken.role === 'owner') {
          navigate('/owner-dashboard');
        } else if (decodedToken.role === 'supplier') {
          navigate('/supplier-dashboard');
        } else {
          navigate('/');
        }
        window.location.reload();
      }
    } catch (err) {
      if (err.response) {
        const message = err.response.data.message || err.response.data.msg || 'An unknown error occurred.';
        setError(`Login failed: ${message}`);
      } else if (err.request) {
        setError('No response from the server. Please ensure the backend is running.');
      } else {
        setError('An unexpected error occurred while setting up the login request.');
      }
      console.error('Login error:', err);
    }
  };

  return (
    <div className="auth-container">
      <h2>Login</h2>
      <LoginForm onSubmit={handleLoginSubmit} error={error} />
      <p>
        Don't have an account? <Link to="/register">Register here</Link>
      </p>
    </div>
  );
};

export default LoginPage;