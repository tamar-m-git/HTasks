import { Schema, model } from "mongoose";
import { Counter } from "./Counter.js";
const orderSchema = new Schema({
  supplierId: {
      type: Schema.Types.ObjectId,
      ref: "User",
      required: true,
      index: true,
    },    listItems: [{
        productId: { type: Schema.Types.ObjectId, ref: "Product", required: true }, 
        quantity: { type: Number, required: true, min:1}
    }],
    status: { type: String, enum: ["created", "in process", "completed"], default: "created" },
    dateOrder: { type: Date, default: Date.now },
    orderNumber: { type: Number, unique: true } 
});
//עדכון אוטומטי למספר הזמנה
orderSchema.pre("save", async function (next) {
  if (this.isNew) {
    const counter = await Counter.findOneAndUpdate(
      { name: "orderNumber" },
      { $inc: { seq: 1 } },
      { new: true, upsert: true }
    );
    this.orderNumber = counter.seq;
  }
  next();
});
export const Order = model("Order", orderSchema);
export default Order;