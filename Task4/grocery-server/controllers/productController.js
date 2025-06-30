import { Product } from "../models/Product.js";
import { User } from "../models/User.js";


export const createProduct = async (req, res) => {
  const { name, price, minQuantity } = req.validatedBody;
  console.log(req.validatedBody);
  const supplierId = req.user.id;

  try {

    const productExists = await Product.findOne({ name, supplierId: supplierId });
    if (productExists) {
      return res
        .status(409)
        .json({ msg: "you  have a product with same name" });
    }
    const product = await Product.create({
      supplierId: supplierId,
      name,
      price,
      minQuantity,
    });
    res.status(201).json({msg:`product ${product.name} created successfully`});
    
    
  } catch (error) {
    console.error(error);
    res.status(500).json({ msg: "server error" });
  }
};









export const getProductBySupplier = async (req, res) => {
  try {
    const supplierId = req.user.id;
    const products = await Product.find({ supplierId })
      .sort({ name: 1 });

    res.json({ count: products.length, products });
  } catch (error) {
    console.error("getProductBySupplier error:", error);
    res.status(500).json({ message: "Server error." });
  }
};