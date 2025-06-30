import { Schema, model } from "mongoose";
const productSchema = new Schema({
  name: { type: String, required: true, unique: true, trim: true },
  price: { type: Number, required: true, min: 0 },
  minQuantity: { type: Number, required: true, min: 0 },
    supplierId: {
    type: Schema.Types.ObjectId,
    ref: "User",
    required: true,
    index: true,
  }
});

productSchema.index({ supplierId: 1, name: 1 }, { unique: true });
export const Product = model("Product", productSchema);
export default Product;