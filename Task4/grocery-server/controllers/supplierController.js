
import { User } from "../models/User.js";
import jwt from "jsonwebtoken";
//get all suppliers
export const getSuppliers = async (req, res) => {
  try {
    const suppliers = await User.find({ role: "supplier" })
      .select("-password")

    res.json({ count: suppliers.length, suppliers });
  } catch (error) {
    console.error("getSuppliers error:", error);
    res.status(500).json({ message: "Internal server error." });
  }
};

