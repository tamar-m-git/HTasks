// frontend/src/components/RegisterPage.js
import React, { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import api from '../api';
import RegisterForm from './auth/RegisterForm'; // <--- תיקון נתיב כאן
import '../styles/Auth.css'; // Styles for authentication pages

const RegisterPage = () => {
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const navigate = useNavigate();

  const handleRegisterSubmit = async (formData) => {
    setError(null);
    setSuccess(null);

    try {
      const response = await api.post('/users/register', formData);
      setSuccess(response.data.message || 'Registration successful!');
      setTimeout(() => {
        navigate('/login');
      }, 2000);
    } catch (err) {
      if (err.response) {
        const message = err.response.data.message || err.response.data.msg || 'An unknown error occurred.';
        setError(`Registration failed: ${message}`);
      } else if (err.request) {
        setError('No response from the server. Please ensure the backend is running.');
      } else {
        setError('An unexpected error occurred while setting up the registration request.');
      }
      console.error('Registration error:', err);
    }
  };

  return (
    <div className="auth-container">
      <h2>Register</h2>
      {success && <p className="success-message">{success}</p>}
      <RegisterForm onSubmit={handleRegisterSubmit} error={error} />
      <p>
        Already have an account? <Link to="/login">Login here</Link>
      </p>
    </div>
  );
};

export default RegisterPage;