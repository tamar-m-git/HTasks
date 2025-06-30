import { Schema, model } from "mongoose";
import bcrypt from "bcrypt";
import express from "express";
const router = express.Router();


const isRequired = function () {
  return this.role === "supplier";
};


const UserSchema  = new Schema({
  userName: { type: String, required: true, unique: true, trim: true },
  role: {
    type: String,
    enum: ["owner", "supplier"],
    required: true,
    trim: true,
  },
    companyName:  { type: String, required: isRequired, trim: true },
    phoneNumber:  { type: String, required: isRequired, trim: true },
    representativeName:  { type: String, required: isRequired, trim: true },
    password: { type: String, required: true }

})


UserSchema.pre("save", async function (next) {
  if (!this.isModified("password")) return next();
  const salt = await bcrypt.genSalt(10);
  this.password = await bcrypt.hash(this.password, salt);
    if (this.role === "owner") {
    this.companyName = undefined;
    this.phone = undefined;
    this.contactName = undefined;
  }
  next();
});

UserSchema.methods.comparePassword = async function (loginPassword) {
  return await bcrypt.compare(loginPassword, this.password);
};
export const User = model("User", UserSchema);
export default User;