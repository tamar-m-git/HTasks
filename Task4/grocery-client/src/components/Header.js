// src/components/Header.js
import React from 'react';
import { Link } from 'react-router-dom';
import '../styles/Header.css';

const Header = ({ onLogout, userRole }) => {
  return (
    <header className="app-header">
      <nav>
        <ul className="nav-links">
          {userRole === 'owner' && (
            <>
              <li><Link to="/owner">Owner Dashboard</Link></li>
              <li><Link to="/owner/suppliers">View Suppliers</Link></li>
              <li><Link to="/owner/add-order">Create Order</Link></li>
              <li><Link to="/owner/orders">View Orders</Link></li>
            </>
          )}
          {userRole === 'supplier' && (
            <>
              <li><Link to="/supplier">Supplier Dashboard</Link></li>
              <li><Link to="/supplier/add-product">Add Product</Link></li>
              <li><Link to="/supplier/products">View Products</Link></li>
              <li><Link to="/supplier/orders">View Orders</Link></li>
            </>
          )}
          {onLogout && (
            <li>
              <button onClick={onLogout} className="logout-button">Logout</button>
            </li>
          )}
        </ul>
      </nav>
    </header>
  );
};

export default Header;