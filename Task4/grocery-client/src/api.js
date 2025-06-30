// frontend/src/api.js
import axios from 'axios';

const api = axios.create({
  baseURL: 'http://localhost:5000', // IMPORTANT: Ensure this matches your backend server's URL
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor to add JWT token to every outgoing request
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor to handle common errors like token expiration
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response && (error.response.status === 401 || error.response.status === 403)) {
      console.error("Authentication error: Token expired or unauthorized. Redirecting to login.");
      // Clear token and role from localStorage
      localStorage.removeItem('token');
      localStorage.removeItem('role');
      // Redirect to login page
      // Using window.location.href to force a full page reload,
      // which clears all React state and ensures the App component re-initializes.
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);

export default api;