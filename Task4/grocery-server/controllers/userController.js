import { User } from "../models/User.js";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import { generateToken } from "../utils/jwt.js";
// register user
export const registerUser = async (req, res) => {
  try {
    const { userName, password, role, companyName, phoneNumber, representativeName } = req.body;
    const existsName = await User.findOne({ userName });
    if (existsName) {
      return res.status(409).json({ message: "Username already in use." });
    }
// לוודא שלא קיים עדיין בעל מכולת- מנהל
    if (role === "owner") {
      const existingOwner = await User.findOne({ role: "owner" });
      if (existingOwner) {
        return res.status(403).json({ message: "Owner already exists. Cannot register another owner." });
      }
    }

        const user = new User({ userName, password, role, companyName, phoneNumber, representativeName });
        await user.save();
      //  const token = generateToken(user);
        res.status(201).json({ message: "Registration successful."});
      
}
  catch (error) {
    console.error("registerUser error:", error);
    res.status(500).json({ message: "Server error." });
  }
};

// login user
export const loginUser = async (req, res) => {
  try {
    const { userName, password } = req.body;
    const user = await User.findOne({ userName });
    if (!user) {
      return res.status(401).json({ message: "Invalid credentials." });
    }

    const isMatch = await bcrypt.compare(password, user.password);
    if (!isMatch) {
      return res.status(401).json({ message: "Invalid credentials." });
    }

    const token = generateToken(user);
    res.json({ accessToken: token });
    console.log("User logged in:", user.userName);
  } catch (error) {
    console.error("loginUser error:", error);
    res.status(500).json({ message: "Server error." });
  }
};

