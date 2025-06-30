// frontend/src/components/auth/LoginForm.js
import React, { useState } from 'react';

const LoginForm = ({ onSubmit, error }) => {
  const [userName, setUserName] = useState('');
  const [password, setPassword] = useState('');

  const handleSubmit = (e) => {
    e.preventDefault();
    onSubmit(userName, password); // Pass username and password to the parent handler
  };

  return (
    <form onSubmit={handleSubmit} className="auth-form">
      {error && <p className="error-message">{error}</p>}
      <div className="form-group">
        <label htmlFor="userName">Username:</label>
        <input
          type="text"
          id="userName"
          name="userName"
          value={userName}
          onChange={(e) => setUserName(e.target.value)}
          required
        />
      </div>
      <div className="form-group">
        <label htmlFor="password">Password:</label>
        <input
          type="password"
          id="password"
          name="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
        />
      </div>
      <button type="submit" className="submit-button">Login</button>
    </form>
  );
};

export default LoginForm;