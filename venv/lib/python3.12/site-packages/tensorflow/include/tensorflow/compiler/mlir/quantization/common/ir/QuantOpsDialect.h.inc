/*===- TableGen'erated file -------------------------------------*- C++ -*-===*\
|*                                                                            *|
|* Dialect Declarations                                                       *|
|*                                                                            *|
|* Automatically generated file, do not edit!                                 *|
|* From: QuantOps.td                                                          *|
|*                                                                            *|
\*===----------------------------------------------------------------------===*/

namespace mlir {
namespace quant {
namespace ir {

class QuantDialect : public ::mlir::Dialect {
  explicit QuantDialect(::mlir::MLIRContext *context);

  void initialize();
  friend class ::mlir::MLIRContext;
public:
  ~QuantDialect() override;
  static constexpr ::llvm::StringLiteral getDialectNamespace() {
    return ::llvm::StringLiteral("quantization");
  }
};
} // namespace ir
} // namespace quant
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::quant::ir::QuantDialect)
