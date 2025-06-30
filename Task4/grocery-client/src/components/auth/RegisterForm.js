// frontend/src/components/auth/RegisterForm.js
import React, { useState } from 'react';

const RegisterForm = ({ onSubmit, error }) => {
  const [formData, setFormData] = useState({
    userName: '',
    password: '',
    role: 'owner', // Default role for new registrations
    companyName: '',
    phoneNumber: '',
    representativeName: '',
  });

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData((prevData) => ({
      ...prevData,
      [name]: value,
    }));
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    onSubmit(formData); // Pass the entire form data to the parent handler
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
          value={formData.userName}
          onChange={handleChange}
          required
        />
      </div>

      <div className="form-group">
        <label htmlFor="password">Password:</label>
        <input
          type="password"
          id="password"
          name="password"
          value={formData.password}
          onChange={handleChange}
          required
        />
      </div>

      <div className="form-group">
        <label htmlFor="role">Role:</label>
        <select id="role" name="role" value={formData.role} onChange={handleChange} required>
          <option value="owner">Store Owner</option>
          <option value="supplier">Supplier</option>
        </select>
      </div>

      {/* Conditional fields for Supplier role */}
      {formData.role === 'supplier' && (
        <>
          <div className="form-group">
            <label htmlFor="companyName">Company Name:</label>
            <input
              type="text"
              id="companyName"
              name="companyName"
              value={formData.companyName}
              onChange={handleChange}
              required // These fields are required when role is 'supplier'
            />
          </div>
          <div className="form-group">
            <label htmlFor="phoneNumber">Phone Number:</label>
            <input
              type="tel"
              id="phoneNumber"
              name="phoneNumber"
              value={formData.phoneNumber}
              onChange={handleChange}
              required
            />
          </div>
          <div className="form-group">
            <label htmlFor="representativeName">Representative Name:</label>
            <input
              type="text"
              id="representativeName"
              name="representativeName"
              value={formData.representativeName}
              onChange={handleChange}
              required
            />
          </div>
        </>
      )}

      <button type="submit" className="submit-button">Register</button>
    </form>
  );
};

export default RegisterForm;