/*===- TableGen'erated file -------------------------------------*- C++ -*-===*\
|*                                                                            *|
|* AttrDef Declarations                                                       *|
|*                                                                            *|
|* Automatically generated file, do not edit!                                 *|
|*                                                                            *|
\*===----------------------------------------------------------------------===*/

#ifdef GET_ATTRDEF_CLASSES
#undef GET_ATTRDEF_CLASSES


namespace mlir {
class AsmParser;
class AsmPrinter;
} // namespace mlir
namespace mlir {
namespace nvgpu {
class TensorMapSwizzleKindAttr;
class TensorMapL2PromoKindAttr;
class TensorMapOOBKindAttr;
class TensorMapInterleaveKindAttr;
namespace detail {
struct TensorMapSwizzleKindAttrStorage;
} // namespace detail
class TensorMapSwizzleKindAttr : public ::mlir::Attribute::AttrBase<TensorMapSwizzleKindAttr, ::mlir::Attribute, detail::TensorMapSwizzleKindAttrStorage> {
public:
  using Base::Base;
  static constexpr ::llvm::StringLiteral name = "nvgpu.swizzle";
  static TensorMapSwizzleKindAttr get(::mlir::MLIRContext *context, ::mlir::nvgpu::TensorMapSwizzleKind value);
  static constexpr ::llvm::StringLiteral getMnemonic() {
    return {"swizzle"};
  }

  static ::mlir::Attribute parse(::mlir::AsmParser &odsParser, ::mlir::Type odsType);
  void print(::mlir::AsmPrinter &odsPrinter) const;
  ::mlir::nvgpu::TensorMapSwizzleKind getValue() const;
};
namespace detail {
struct TensorMapL2PromoKindAttrStorage;
} // namespace detail
class TensorMapL2PromoKindAttr : public ::mlir::Attribute::AttrBase<TensorMapL2PromoKindAttr, ::mlir::Attribute, detail::TensorMapL2PromoKindAttrStorage> {
public:
  using Base::Base;
  static constexpr ::llvm::StringLiteral name = "nvgpu.l2promo";
  static TensorMapL2PromoKindAttr get(::mlir::MLIRContext *context, ::mlir::nvgpu::TensorMapL2PromoKind value);
  static constexpr ::llvm::StringLiteral getMnemonic() {
    return {"l2promo"};
  }

  static ::mlir::Attribute parse(::mlir::AsmParser &odsParser, ::mlir::Type odsType);
  void print(::mlir::AsmPrinter &odsPrinter) const;
  ::mlir::nvgpu::TensorMapL2PromoKind getValue() const;
};
namespace detail {
struct TensorMapOOBKindAttrStorage;
} // namespace detail
class TensorMapOOBKindAttr : public ::mlir::Attribute::AttrBase<TensorMapOOBKindAttr, ::mlir::Attribute, detail::TensorMapOOBKindAttrStorage> {
public:
  using Base::Base;
  static constexpr ::llvm::StringLiteral name = "nvgpu.oob";
  static TensorMapOOBKindAttr get(::mlir::MLIRContext *context, ::mlir::nvgpu::TensorMapOOBKind value);
  static constexpr ::llvm::StringLiteral getMnemonic() {
    return {"oob"};
  }

  static ::mlir::Attribute parse(::mlir::AsmParser &odsParser, ::mlir::Type odsType);
  void print(::mlir::AsmPrinter &odsPrinter) const;
  ::mlir::nvgpu::TensorMapOOBKind getValue() const;
};
namespace detail {
struct TensorMapInterleaveKindAttrStorage;
} // namespace detail
class TensorMapInterleaveKindAttr : public ::mlir::Attribute::AttrBase<TensorMapInterleaveKindAttr, ::mlir::Attribute, detail::TensorMapInterleaveKindAttrStorage> {
public:
  using Base::Base;
  static constexpr ::llvm::StringLiteral name = "nvgpu.interleave";
  static TensorMapInterleaveKindAttr get(::mlir::MLIRContext *context, ::mlir::nvgpu::TensorMapInterleaveKind value);
  static constexpr ::llvm::StringLiteral getMnemonic() {
    return {"interleave"};
  }

  static ::mlir::Attribute parse(::mlir::AsmParser &odsParser, ::mlir::Type odsType);
  void print(::mlir::AsmPrinter &odsPrinter) const;
  ::mlir::nvgpu::TensorMapInterleaveKind getValue() const;
};
} // namespace nvgpu
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::nvgpu::TensorMapSwizzleKindAttr)
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::nvgpu::TensorMapL2PromoKindAttr)
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::nvgpu::TensorMapOOBKindAttr)
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::nvgpu::TensorMapInterleaveKindAttr)

#endif  // GET_ATTRDEF_CLASSES

