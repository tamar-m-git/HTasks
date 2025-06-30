// frontend/src/App.js
import React from 'react';
import { BrowserRouter as Router } from 'react-router-dom'; // ייבוא ה-Router לכאן בלבד
import AppContent from './AppContent'; // ייבוא הקומפוננטה החדשה שיצרנו

import './styles/App.css'; // השאר אם יש לך קובץ CSS כללי ל-App

const App = () => {
  return (
    <Router> {/* ה-Router עוטף כעת את AppContent */}
      <AppContent />
    </Router>
  );
};

export default App;