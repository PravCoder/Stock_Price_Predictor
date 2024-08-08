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
namespace mesh {
class ReductionKindAttr;
class MeshShardingAttr;
namespace detail {
struct ReductionKindAttrStorage;
} // namespace detail
class ReductionKindAttr : public ::mlir::Attribute::AttrBase<ReductionKindAttr, ::mlir::Attribute, detail::ReductionKindAttrStorage> {
public:
  using Base::Base;
  static constexpr ::llvm::StringLiteral name = "mesh.partial";
  static ReductionKindAttr get(::mlir::MLIRContext *context, ::mlir::mesh::ReductionKind value);
  static constexpr ::llvm::StringLiteral getMnemonic() {
    return {"partial"};
  }

  static ::mlir::Attribute parse(::mlir::AsmParser &odsParser, ::mlir::Type odsType);
  void print(::mlir::AsmPrinter &odsPrinter) const;
  ::mlir::mesh::ReductionKind getValue() const;
};
namespace detail {
struct MeshShardingAttrStorage;
} // namespace detail
class MeshShardingAttr : public ::mlir::Attribute::AttrBase<MeshShardingAttr, ::mlir::Attribute, detail::MeshShardingAttrStorage> {
public:
  using Base::Base;
  bool operator==(::mlir::Attribute rhs) const;
  bool operator!=(::mlir::Attribute rhs) const;
  bool operator==(::mlir::mesh::MeshShardingAttr rhs) const;
  bool operator!=(::mlir::mesh::MeshShardingAttr rhs) const;
  static constexpr ::llvm::StringLiteral name = "mesh.shard";
  using Base::getChecked;
  static MeshShardingAttr get(::mlir::MLIRContext *context, ::mlir::FlatSymbolRefAttr mesh, ::llvm::ArrayRef<MeshAxesAttr> split_axes, ::llvm::ArrayRef<MeshAxis> partial_axes, ::mlir::mesh::ReductionKind partial_type);
  static MeshShardingAttr getChecked(::llvm::function_ref<::mlir::InFlightDiagnostic()> emitError, ::mlir::MLIRContext *context, ::mlir::FlatSymbolRefAttr mesh, ::llvm::ArrayRef<MeshAxesAttr> split_axes, ::llvm::ArrayRef<MeshAxis> partial_axes, ::mlir::mesh::ReductionKind partial_type);
  static MeshShardingAttr get(::mlir::MLIRContext *context, FlatSymbolRefAttr mesh, ArrayRef<SmallVector<MeshAxis>> split_axes, ArrayRef<MeshAxis> partial_axes, mesh::ReductionKind partial_type);
  static MeshShardingAttr getChecked(::llvm::function_ref<::mlir::InFlightDiagnostic()> emitError, ::mlir::MLIRContext *context, FlatSymbolRefAttr mesh, ArrayRef<SmallVector<MeshAxis>> split_axes, ArrayRef<MeshAxis> partial_axes, mesh::ReductionKind partial_type);
  static MeshShardingAttr get(::mlir::MLIRContext *context, FlatSymbolRefAttr mesh, ArrayRef<SmallVector<MeshAxis>> split_axes);
  static MeshShardingAttr getChecked(::llvm::function_ref<::mlir::InFlightDiagnostic()> emitError, ::mlir::MLIRContext *context, FlatSymbolRefAttr mesh, ArrayRef<SmallVector<MeshAxis>> split_axes);
  static ::mlir::LogicalResult verify(::llvm::function_ref<::mlir::InFlightDiagnostic()> emitError, ::mlir::FlatSymbolRefAttr mesh, ::llvm::ArrayRef<MeshAxesAttr> split_axes, ::llvm::ArrayRef<MeshAxis> partial_axes, ::mlir::mesh::ReductionKind partial_type);
  static constexpr ::llvm::StringLiteral getMnemonic() {
    return {"shard"};
  }

  static ::mlir::Attribute parse(::mlir::AsmParser &odsParser, ::mlir::Type odsType);
  void print(::mlir::AsmPrinter &odsPrinter) const;
  ::mlir::FlatSymbolRefAttr getMesh() const;
  ::llvm::ArrayRef<MeshAxesAttr> getSplitAxes() const;
  ::llvm::ArrayRef<MeshAxis> getPartialAxes() const;
  ::mlir::mesh::ReductionKind getPartialType() const;
};
} // namespace mesh
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::mesh::ReductionKindAttr)
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::mesh::MeshShardingAttr)

#endif  // GET_ATTRDEF_CLASSES

