// server.js
import express from "express";
import connectDB from "./config/ConnectToDB.js";
import dotenv from "dotenv";
import cors from "cors";


import UserRouter from "./routes/UserRoutes.js";
import SupplierRouter from "./routes/SupplierRoutes.js";
import ProductRouter from "./routes/ProductRoutes.js";
import OrderRouter from "./routes/OrderRoutes.js";


dotenv.config();

const app = express();

app.use(express.json());
app.use(cors());

connectDB();

app.use("/users", UserRouter);
app.use('/suppliers', SupplierRouter);
app.use("/products", ProductRouter);
app.use("/orders", OrderRouter);

app.get("/", (req, res) => {
  res.send("API is running...");
});

app.use((err, req, res, next) => {
  console.error(err.stack);
  res
    .status(500)
    .json({
      msg: "something isnt working right",
      error: err.message,
      stack: err.stack,
    });
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`server running on port ${PORT}`));

