
import mongoose from "mongoose";
import dotenv from "dotenv";
dotenv.config();

const connectDB = async () => {
  try {
    if (!process.env.MONGO_URI) {
      console.error("there isnt MONGO_URI");
      process.exit(1);
    }
    await mongoose.connect(process.env.MONGO_URI);
    console.log("success connect to mongoDB");
  } catch (err) {
    console.error(`error in connect to mongoDB: ${err.message}`);
    process.exit(1);
  }
};

export default connectDB;
// This code connects to a MongoDB database using Mongoose.
