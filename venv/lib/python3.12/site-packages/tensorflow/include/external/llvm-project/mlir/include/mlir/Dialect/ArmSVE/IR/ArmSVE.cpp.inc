/*===- TableGen'erated file -------------------------------------*- C++ -*-===*\
|*                                                                            *|
|* Op Definitions                                                             *|
|*                                                                            *|
|* Automatically generated file, do not edit!                                 *|
|* From: ArmSVE.td                                                            *|
|*                                                                            *|
\*===----------------------------------------------------------------------===*/

#ifdef GET_OP_LIST
#undef GET_OP_LIST

::mlir::arm_sve::ConvertFromSvboolIntrOp,
::mlir::arm_sve::ConvertFromSvboolOp,
::mlir::arm_sve::ConvertToSvboolIntrOp,
::mlir::arm_sve::ConvertToSvboolOp,
::mlir::arm_sve::ScalableMaskedAddFIntrOp,
::mlir::arm_sve::ScalableMaskedAddFOp,
::mlir::arm_sve::ScalableMaskedAddIIntrOp,
::mlir::arm_sve::ScalableMaskedAddIOp,
::mlir::arm_sve::ScalableMaskedDivFIntrOp,
::mlir::arm_sve::ScalableMaskedDivFOp,
::mlir::arm_sve::ScalableMaskedMulFIntrOp,
::mlir::arm_sve::ScalableMaskedMulFOp,
::mlir::arm_sve::ScalableMaskedMulIIntrOp,
::mlir::arm_sve::ScalableMaskedMulIOp,
::mlir::arm_sve::ScalableMaskedSDivIIntrOp,
::mlir::arm_sve::ScalableMaskedSDivIOp,
::mlir::arm_sve::ScalableMaskedSubFIntrOp,
::mlir::arm_sve::ScalableMaskedSubFOp,
::mlir::arm_sve::ScalableMaskedSubIIntrOp,
::mlir::arm_sve::ScalableMaskedSubIOp,
::mlir::arm_sve::ScalableMaskedUDivIIntrOp,
::mlir::arm_sve::ScalableMaskedUDivIOp,
::mlir::arm_sve::SdotIntrOp,
::mlir::arm_sve::SdotOp,
::mlir::arm_sve::SmmlaIntrOp,
::mlir::arm_sve::SmmlaOp,
::mlir::arm_sve::UdotIntrOp,
::mlir::arm_sve::UdotOp,
::mlir::arm_sve::UmmlaIntrOp,
::mlir::arm_sve::UmmlaOp,
::mlir::arm_sve::ZipX2IntrOp,
::mlir::arm_sve::ZipX2Op,
::mlir::arm_sve::ZipX4IntrOp,
::mlir::arm_sve::ZipX4Op
#endif  // GET_OP_LIST

#ifdef GET_OP_CLASSES
#undef GET_OP_CLASSES


//===----------------------------------------------------------------------===//
// Local Utility Method Definitions
//===----------------------------------------------------------------------===//

namespace mlir {
namespace arm_sve {

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE0(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!((((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ((::llvm::cast<::mlir::VectorType>(type).getRank()
                           == 1))) && (((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ([](::mlir::Type elementType) { return (elementType.isSignlessInteger(1)); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))) && (((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ((::llvm::cast<::mlir::VectorType>(type).getNumElements()
                           == 16))))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be  of ranks 1scalable vector of 1-bit signless integer values of length 16, but got " << type;
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE1(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!((::mlir::LLVM::isCompatibleOuterType(type)))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be LLVM dialect-compatible type, but got " << type;
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE2(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!(((((::llvm::isa<::mlir::VectorType>(type))) && ((::llvm::cast<::mlir::VectorType>(type).getRank() > 0)) && ((::llvm::cast<::mlir::VectorType>(type).getScalableDims().back())) && ((!llvm::is_contained(::llvm::cast<::mlir::VectorType>(type).getScalableDims().drop_back(), true)))) && ([](::mlir::Type elementType) { return (elementType.isSignlessInteger(1)); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))) && (((::llvm::cast<::mlir::ShapedType>(type).getRank() >= 1)) && ((::llvm::is_contained(ArrayRef<int64_t>({16}), ::llvm::cast<::mlir::ShapedType>(type).getDimSize(::llvm::cast<::mlir::ShapedType>(type).getRank() + -1))))))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be trailing scalable vector of 1-bit signless integer values with dim -1 having a size of {16}, but got " << type;
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE3(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!(((((::llvm::isa<::mlir::VectorType>(type))) && ((::llvm::cast<::mlir::VectorType>(type).getRank() > 0)) && ((::llvm::cast<::mlir::VectorType>(type).getScalableDims().back())) && ((!llvm::is_contained(::llvm::cast<::mlir::VectorType>(type).getScalableDims().drop_back(), true)))) && ([](::mlir::Type elementType) { return (elementType.isSignlessInteger(1)); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))) && (((::llvm::cast<::mlir::ShapedType>(type).getRank() >= 1)) && ((::llvm::is_contained(ArrayRef<int64_t>({16, 8, 4, 2, 1}), ::llvm::cast<::mlir::ShapedType>(type).getDimSize(::llvm::cast<::mlir::ShapedType>(type).getRank() + -1))))))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be trailing scalable vector of 1-bit signless integer values with dim -1 having a size of {16, 8, 4, 2, 1}, but got " << type;
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE4(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!((((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ((::llvm::cast<::mlir::VectorType>(type).getRank()
                           == 1))) && (((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ([](::mlir::Type elementType) { return (elementType.isSignlessInteger(1)); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))) && (((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && (((::llvm::cast<::mlir::VectorType>(type).getNumElements()
                           == 16)) || ((::llvm::cast<::mlir::VectorType>(type).getNumElements()
                           == 8)) || ((::llvm::cast<::mlir::VectorType>(type).getNumElements()
                           == 4)) || ((::llvm::cast<::mlir::VectorType>(type).getNumElements()
                           == 2)) || ((::llvm::cast<::mlir::VectorType>(type).getNumElements()
                           == 1)))))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be  of ranks 1scalable vector of 1-bit signless integer values of length 16/8/4/2/1, but got " << type;
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE5(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!(((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ([](::mlir::Type elementType) { return (true); }(::llvm::cast<::mlir::ShapedType>(type).getElementType())))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be scalable vector of any type values, but got " << type;
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE6(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!(((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ([](::mlir::Type elementType) { return (elementType.isSignlessInteger(1)); }(::llvm::cast<::mlir::ShapedType>(type).getElementType())))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be scalable vector of 1-bit signless integer values, but got " << type;
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE7(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!(((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ([](::mlir::Type elementType) { return (::llvm::isa<::mlir::FloatType>(elementType)); }(::llvm::cast<::mlir::ShapedType>(type).getElementType())))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be scalable vector of floating-point values, but got " << type;
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE8(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!(((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ([](::mlir::Type elementType) { return ((elementType.isSignlessInteger(8))) || ((elementType.isSignlessInteger(16))) || ((elementType.isSignlessInteger(32))) || ((elementType.isSignlessInteger(64))); }(::llvm::cast<::mlir::ShapedType>(type).getElementType())))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be scalable vector of 8-bit signless integer or 16-bit signless integer or 32-bit signless integer or 64-bit signless integer values, but got " << type;
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE9(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!((((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ([](::mlir::Type elementType) { return ((elementType.isSignlessInteger(32))) || ((elementType.isSignlessInteger(64))); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))) && (((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && (((::llvm::cast<::mlir::VectorType>(type).getNumElements()
                           == 4)) || ((::llvm::cast<::mlir::VectorType>(type).getNumElements()
                           == 2)))))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be scalable vector of 32-bit signless integer or 64-bit signless integer values of length 4/2, but got " << type;
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE10(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!((((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ([](::mlir::Type elementType) { return ((elementType.isSignlessInteger(8))) || ((elementType.isSignlessInteger(16))); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))) && (((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && (((::llvm::cast<::mlir::VectorType>(type).getNumElements()
                           == 16)) || ((::llvm::cast<::mlir::VectorType>(type).getNumElements()
                           == 8)))))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be scalable vector of 8-bit signless integer or 16-bit signless integer values of length 16/8, but got " << type;
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE11(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!((((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ([](::mlir::Type elementType) { return (elementType.isSignlessInteger(32)); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))) && (((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ((::llvm::cast<::mlir::VectorType>(type).getNumElements()
                           == 4))))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be scalable vector of 32-bit signless integer values of length 4, but got " << type;
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE12(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!((((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ([](::mlir::Type elementType) { return (elementType.isSignlessInteger(8)); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))) && (((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable())) && ((::llvm::cast<::mlir::VectorType>(type).getNumElements()
                           == 16))))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be scalable vector of 8-bit signless integer values of length 16, but got " << type;
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ArmSVE13(
    ::mlir::Operation *op, ::mlir::Type type, ::llvm::StringRef valueKind,
    unsigned valueIndex) {
  if (!(((((::llvm::cast<::mlir::VectorType>(type).getShape() == ArrayRef<int64_t>({2}))) && ((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable()))) && ([](::mlir::Type elementType) { return ((elementType.isSignlessInteger(64))) || ((elementType.isF64())); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))) || ((((::llvm::cast<::mlir::VectorType>(type).getShape() == ArrayRef<int64_t>({4}))) && ((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable()))) && ([](::mlir::Type elementType) { return ((elementType.isSignlessInteger(32))) || ((elementType.isF32())); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))) || ((((::llvm::cast<::mlir::VectorType>(type).getShape() == ArrayRef<int64_t>({8}))) && ((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable()))) && ([](::mlir::Type elementType) { return ((elementType.isSignlessInteger(16))) || ((elementType.isF16())) || ((elementType.isBF16())); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))) || ((((::llvm::cast<::mlir::VectorType>(type).getShape() == ArrayRef<int64_t>({16}))) && ((::llvm::isa<::mlir::VectorType>(type) &&
                  ::llvm::cast<VectorType>(type).isScalable()))) && ([](::mlir::Type elementType) { return (elementType.isSignlessInteger(8)); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))))) {
    return op->emitOpError(valueKind) << " #" << valueIndex
        << " must be an SVE vector with element size <= 64-bit, but got " << type;
  }
  return ::mlir::success();
}
} // namespace arm_sve
} // namespace mlir
namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ConvertFromSvboolIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ConvertFromSvboolIntrOpAdaptor::ConvertFromSvboolIntrOpAdaptor(ConvertFromSvboolIntrOp op) : ConvertFromSvboolIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ConvertFromSvboolIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ConvertFromSvboolIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value svbool) {
  odsState.addOperands(svbool);
  odsState.addTypes(res);
}

void ConvertFromSvboolIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value svbool) {
  odsState.addOperands(svbool);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ConvertFromSvboolIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 1u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ConvertFromSvboolIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE0(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((::llvm::isa<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()) &&
                  ::llvm::cast<VectorType>((*this->getODSResults(0).begin()).getType()).isScalable())) && ((::llvm::cast<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()).getRank()
                           == 1))) && (((::llvm::isa<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()) &&
                  ::llvm::cast<VectorType>((*this->getODSResults(0).begin()).getType()).isScalable())) && ([](::mlir::Type elementType) { return (elementType.isSignlessInteger(1)); }(::llvm::cast<::mlir::ShapedType>((*this->getODSResults(0).begin()).getType()).getElementType()))) && (((::llvm::isa<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()) &&
                  ::llvm::cast<VectorType>((*this->getODSResults(0).begin()).getType()).isScalable())) && (((::llvm::cast<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()).getNumElements()
                           == 16)) || ((::llvm::cast<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()).getNumElements()
                           == 8)) || ((::llvm::cast<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()).getNumElements()
                           == 4)) || ((::llvm::cast<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()).getNumElements()
                           == 2)) || ((::llvm::cast<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()).getNumElements()
                           == 1))))))
    return emitOpError("failed to verify that 'res' is  of ranks 1scalable vector of 1-bit signless integer values of length 16/8/4/2/1");
  return ::mlir::success();
}

::mlir::LogicalResult ConvertFromSvboolIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ConvertFromSvboolIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ConvertFromSvboolOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ConvertFromSvboolOpAdaptor::ConvertFromSvboolOpAdaptor(ConvertFromSvboolOp op) : ConvertFromSvboolOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ConvertFromSvboolOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ConvertFromSvboolOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type result, ::mlir::Value source) {
  odsState.addOperands(source);
  odsState.addTypes(result);
}

void ConvertFromSvboolOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value source) {
  odsState.addOperands(source);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ConvertFromSvboolOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 1u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ConvertFromSvboolOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE2(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE3(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((std::equal_to<>()(VectorType(VectorType::Builder(::llvm::cast<VectorType>((*this->getODSResults(0).begin()).getType())).setDim(::llvm::cast<VectorType>((*this->getODSResults(0).begin()).getType()).getRank() - 1, 16)), (*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that expected corresponding svbool type widened to [16]xi1");
  return ::mlir::success();
}

::mlir::LogicalResult ConvertFromSvboolOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult ConvertFromSvboolOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand sourceRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> sourceOperands(&sourceRawOperand, 1);  ::llvm::SMLoc sourceOperandsLoc;
  (void)sourceOperandsLoc;
  ::mlir::Type resultRawType{};
  ::llvm::ArrayRef<::mlir::Type> resultTypes(&resultRawType, 1);

  sourceOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(sourceRawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    resultRawType = type;
  }
  for (::mlir::Type type : resultTypes) {
    (void)type;
    if (!(((((::llvm::isa<::mlir::VectorType>(type))) && ((::llvm::cast<::mlir::VectorType>(type).getRank() > 0)) && ((::llvm::cast<::mlir::VectorType>(type).getScalableDims().back())) && ((!llvm::is_contained(::llvm::cast<::mlir::VectorType>(type).getScalableDims().drop_back(), true)))) && ([](::mlir::Type elementType) { return (elementType.isSignlessInteger(1)); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))) && (((::llvm::cast<::mlir::ShapedType>(type).getRank() >= 1)) && ((::llvm::is_contained(ArrayRef<int64_t>({16, 8, 4, 2, 1}), ::llvm::cast<::mlir::ShapedType>(type).getDimSize(::llvm::cast<::mlir::ShapedType>(type).getRank() + -1))))))) {
      return parser.emitError(parser.getNameLoc()) << "'result' must be trailing scalable vector of 1-bit signless integer values with dim -1 having a size of {16, 8, 4, 2, 1}, but got " << type;
    }
  }
  result.addTypes(resultTypes);
  if (parser.resolveOperands(sourceOperands, VectorType(VectorType::Builder(::llvm::cast<VectorType>(resultTypes[0])).setDim(::llvm::cast<VectorType>(resultTypes[0]).getRank() - 1, 16)), sourceOperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void ConvertFromSvboolOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getSource();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getResult().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

void ConvertFromSvboolOp::getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects) {
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ConvertFromSvboolOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ConvertToSvboolIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ConvertToSvboolIntrOpAdaptor::ConvertToSvboolIntrOpAdaptor(ConvertToSvboolIntrOp op) : ConvertToSvboolIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ConvertToSvboolIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ConvertToSvboolIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value mask) {
  odsState.addOperands(mask);
  odsState.addTypes(res);
}

void ConvertToSvboolIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value mask) {
  odsState.addOperands(mask);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ConvertToSvboolIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 1u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ConvertToSvboolIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE4(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((::llvm::isa<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()) &&
                  ::llvm::cast<VectorType>((*this->getODSResults(0).begin()).getType()).isScalable())) && ((::llvm::cast<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()).getRank()
                           == 1))) && (((::llvm::isa<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()) &&
                  ::llvm::cast<VectorType>((*this->getODSResults(0).begin()).getType()).isScalable())) && ([](::mlir::Type elementType) { return (elementType.isSignlessInteger(1)); }(::llvm::cast<::mlir::ShapedType>((*this->getODSResults(0).begin()).getType()).getElementType()))) && (((::llvm::isa<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()) &&
                  ::llvm::cast<VectorType>((*this->getODSResults(0).begin()).getType()).isScalable())) && ((::llvm::cast<::mlir::VectorType>((*this->getODSResults(0).begin()).getType()).getNumElements()
                           == 16)))))
    return emitOpError("failed to verify that 'res' is  of ranks 1scalable vector of 1-bit signless integer values of length 16");
  return ::mlir::success();
}

::mlir::LogicalResult ConvertToSvboolIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ConvertToSvboolIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ConvertToSvboolOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ConvertToSvboolOpAdaptor::ConvertToSvboolOpAdaptor(ConvertToSvboolOp op) : ConvertToSvboolOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ConvertToSvboolOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ConvertToSvboolOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type result, ::mlir::Value source) {
  odsState.addOperands(source);
  odsState.addTypes(result);
}

void ConvertToSvboolOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value source) {
  odsState.addOperands(source);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ConvertToSvboolOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 1u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ConvertToSvboolOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE3(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE2(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((std::equal_to<>()(VectorType(VectorType::Builder(::llvm::cast<VectorType>((*this->getODSOperands(0).begin()).getType())).setDim(::llvm::cast<VectorType>((*this->getODSOperands(0).begin()).getType()).getRank() - 1, 16)), (*this->getODSResults(0).begin()).getType()))))
    return emitOpError("failed to verify that expected corresponding svbool type widened to [16]xi1");
  return ::mlir::success();
}

::mlir::LogicalResult ConvertToSvboolOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult ConvertToSvboolOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand sourceRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> sourceOperands(&sourceRawOperand, 1);  ::llvm::SMLoc sourceOperandsLoc;
  (void)sourceOperandsLoc;
  ::mlir::Type sourceRawType{};
  ::llvm::ArrayRef<::mlir::Type> sourceTypes(&sourceRawType, 1);

  sourceOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(sourceRawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    sourceRawType = type;
  }
  for (::mlir::Type type : sourceTypes) {
    (void)type;
    if (!(((((::llvm::isa<::mlir::VectorType>(type))) && ((::llvm::cast<::mlir::VectorType>(type).getRank() > 0)) && ((::llvm::cast<::mlir::VectorType>(type).getScalableDims().back())) && ((!llvm::is_contained(::llvm::cast<::mlir::VectorType>(type).getScalableDims().drop_back(), true)))) && ([](::mlir::Type elementType) { return (elementType.isSignlessInteger(1)); }(::llvm::cast<::mlir::ShapedType>(type).getElementType()))) && (((::llvm::cast<::mlir::ShapedType>(type).getRank() >= 1)) && ((::llvm::is_contained(ArrayRef<int64_t>({16, 8, 4, 2, 1}), ::llvm::cast<::mlir::ShapedType>(type).getDimSize(::llvm::cast<::mlir::ShapedType>(type).getRank() + -1))))))) {
      return parser.emitError(parser.getNameLoc()) << "'source' must be trailing scalable vector of 1-bit signless integer values with dim -1 having a size of {16, 8, 4, 2, 1}, but got " << type;
    }
  }
  result.addTypes(VectorType(VectorType::Builder(::llvm::cast<VectorType>(sourceTypes[0])).setDim(::llvm::cast<VectorType>(sourceTypes[0]).getRank() - 1, 16)));
  if (parser.resolveOperands(sourceOperands, sourceTypes, sourceOperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void ConvertToSvboolOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getSource();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getSource().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

void ConvertToSvboolOp::getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects) {
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ConvertToSvboolOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedAddFIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedAddFIntrOpAdaptor::ScalableMaskedAddFIntrOpAdaptor(ScalableMaskedAddFIntrOp op) : ScalableMaskedAddFIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedAddFIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedAddFIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  odsState.addTypes(res);
}

void ScalableMaskedAddFIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedAddFIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedAddFIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedAddFIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedAddFIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedAddFOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedAddFOpAdaptor::ScalableMaskedAddFOpAdaptor(ScalableMaskedAddFOp op) : ScalableMaskedAddFOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedAddFOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedAddFOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  odsState.addTypes(res);
}

void ScalableMaskedAddFOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedAddFOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedAddFOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE6(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE7(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE7(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE7(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()))))
    return emitOpError("failed to verify that all of {src1, src2, res} have same type");
  if (!((std::equal_to<>()(getI1SameShape((*this->getODSOperands(1).begin()).getType()), (*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that mask has i1 element type and same shape as operands");
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedAddFOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult ScalableMaskedAddFOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand maskRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> maskOperands(&maskRawOperand, 1);  ::llvm::SMLoc maskOperandsLoc;
  (void)maskOperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src1Operands(&src1RawOperand, 1);  ::llvm::SMLoc src1OperandsLoc;
  (void)src1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src2Operands(&src2RawOperand, 1);  ::llvm::SMLoc src2OperandsLoc;
  (void)src2OperandsLoc;
  ::mlir::Type maskRawType{};
  ::llvm::ArrayRef<::mlir::Type> maskTypes(&maskRawType, 1);
  ::mlir::Type resRawType{};
  ::llvm::ArrayRef<::mlir::Type> resTypes(&resRawType, 1);

  maskOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(maskRawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    maskRawType = type;
  }
  if (parser.parseComma())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    resRawType = type;
  }
  result.addTypes(resTypes);
  if (parser.resolveOperands(maskOperands, maskTypes, maskOperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src1Operands, resTypes[0], src1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src2Operands, resTypes[0], src2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void ScalableMaskedAddFOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getMask();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getMask().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
  _odsPrinter << ",";
  _odsPrinter << ' ';
  {
    auto type = getRes().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedAddFOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedAddIIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedAddIIntrOpAdaptor::ScalableMaskedAddIIntrOpAdaptor(ScalableMaskedAddIIntrOp op) : ScalableMaskedAddIIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedAddIIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedAddIIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  odsState.addTypes(res);
}

void ScalableMaskedAddIIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedAddIIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedAddIIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedAddIIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedAddIIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedAddIOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedAddIOpAdaptor::ScalableMaskedAddIOpAdaptor(ScalableMaskedAddIOp op) : ScalableMaskedAddIOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedAddIOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedAddIOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  odsState.addTypes(res);
}

void ScalableMaskedAddIOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedAddIOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedAddIOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE6(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()))))
    return emitOpError("failed to verify that all of {src1, src2, res} have same type");
  if (!((std::equal_to<>()(getI1SameShape((*this->getODSOperands(1).begin()).getType()), (*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that mask has i1 element type and same shape as operands");
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedAddIOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult ScalableMaskedAddIOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand maskRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> maskOperands(&maskRawOperand, 1);  ::llvm::SMLoc maskOperandsLoc;
  (void)maskOperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src1Operands(&src1RawOperand, 1);  ::llvm::SMLoc src1OperandsLoc;
  (void)src1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src2Operands(&src2RawOperand, 1);  ::llvm::SMLoc src2OperandsLoc;
  (void)src2OperandsLoc;
  ::mlir::Type maskRawType{};
  ::llvm::ArrayRef<::mlir::Type> maskTypes(&maskRawType, 1);
  ::mlir::Type resRawType{};
  ::llvm::ArrayRef<::mlir::Type> resTypes(&resRawType, 1);

  maskOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(maskRawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    maskRawType = type;
  }
  if (parser.parseComma())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    resRawType = type;
  }
  result.addTypes(resTypes);
  if (parser.resolveOperands(maskOperands, maskTypes, maskOperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src1Operands, resTypes[0], src1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src2Operands, resTypes[0], src2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void ScalableMaskedAddIOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getMask();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getMask().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
  _odsPrinter << ",";
  _odsPrinter << ' ';
  {
    auto type = getRes().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedAddIOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedDivFIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedDivFIntrOpAdaptor::ScalableMaskedDivFIntrOpAdaptor(ScalableMaskedDivFIntrOp op) : ScalableMaskedDivFIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedDivFIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedDivFIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  odsState.addTypes(res);
}

void ScalableMaskedDivFIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedDivFIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedDivFIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedDivFIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedDivFIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedDivFOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedDivFOpAdaptor::ScalableMaskedDivFOpAdaptor(ScalableMaskedDivFOp op) : ScalableMaskedDivFOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedDivFOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedDivFOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  odsState.addTypes(res);
}

void ScalableMaskedDivFOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedDivFOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedDivFOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE6(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE7(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE7(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE7(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()))))
    return emitOpError("failed to verify that all of {src1, src2, res} have same type");
  if (!((std::equal_to<>()(getI1SameShape((*this->getODSOperands(1).begin()).getType()), (*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that mask has i1 element type and same shape as operands");
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedDivFOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult ScalableMaskedDivFOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand maskRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> maskOperands(&maskRawOperand, 1);  ::llvm::SMLoc maskOperandsLoc;
  (void)maskOperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src1Operands(&src1RawOperand, 1);  ::llvm::SMLoc src1OperandsLoc;
  (void)src1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src2Operands(&src2RawOperand, 1);  ::llvm::SMLoc src2OperandsLoc;
  (void)src2OperandsLoc;
  ::mlir::Type maskRawType{};
  ::llvm::ArrayRef<::mlir::Type> maskTypes(&maskRawType, 1);
  ::mlir::Type resRawType{};
  ::llvm::ArrayRef<::mlir::Type> resTypes(&resRawType, 1);

  maskOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(maskRawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    maskRawType = type;
  }
  if (parser.parseComma())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    resRawType = type;
  }
  result.addTypes(resTypes);
  if (parser.resolveOperands(maskOperands, maskTypes, maskOperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src1Operands, resTypes[0], src1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src2Operands, resTypes[0], src2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void ScalableMaskedDivFOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getMask();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getMask().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
  _odsPrinter << ",";
  _odsPrinter << ' ';
  {
    auto type = getRes().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedDivFOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedMulFIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedMulFIntrOpAdaptor::ScalableMaskedMulFIntrOpAdaptor(ScalableMaskedMulFIntrOp op) : ScalableMaskedMulFIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedMulFIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedMulFIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  odsState.addTypes(res);
}

void ScalableMaskedMulFIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedMulFIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedMulFIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedMulFIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedMulFIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedMulFOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedMulFOpAdaptor::ScalableMaskedMulFOpAdaptor(ScalableMaskedMulFOp op) : ScalableMaskedMulFOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedMulFOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedMulFOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  odsState.addTypes(res);
}

void ScalableMaskedMulFOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedMulFOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedMulFOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE6(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE7(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE7(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE7(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()))))
    return emitOpError("failed to verify that all of {src1, src2, res} have same type");
  if (!((std::equal_to<>()(getI1SameShape((*this->getODSOperands(1).begin()).getType()), (*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that mask has i1 element type and same shape as operands");
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedMulFOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult ScalableMaskedMulFOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand maskRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> maskOperands(&maskRawOperand, 1);  ::llvm::SMLoc maskOperandsLoc;
  (void)maskOperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src1Operands(&src1RawOperand, 1);  ::llvm::SMLoc src1OperandsLoc;
  (void)src1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src2Operands(&src2RawOperand, 1);  ::llvm::SMLoc src2OperandsLoc;
  (void)src2OperandsLoc;
  ::mlir::Type maskRawType{};
  ::llvm::ArrayRef<::mlir::Type> maskTypes(&maskRawType, 1);
  ::mlir::Type resRawType{};
  ::llvm::ArrayRef<::mlir::Type> resTypes(&resRawType, 1);

  maskOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(maskRawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    maskRawType = type;
  }
  if (parser.parseComma())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    resRawType = type;
  }
  result.addTypes(resTypes);
  if (parser.resolveOperands(maskOperands, maskTypes, maskOperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src1Operands, resTypes[0], src1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src2Operands, resTypes[0], src2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void ScalableMaskedMulFOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getMask();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getMask().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
  _odsPrinter << ",";
  _odsPrinter << ' ';
  {
    auto type = getRes().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedMulFOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedMulIIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedMulIIntrOpAdaptor::ScalableMaskedMulIIntrOpAdaptor(ScalableMaskedMulIIntrOp op) : ScalableMaskedMulIIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedMulIIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedMulIIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  odsState.addTypes(res);
}

void ScalableMaskedMulIIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedMulIIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedMulIIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedMulIIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedMulIIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedMulIOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedMulIOpAdaptor::ScalableMaskedMulIOpAdaptor(ScalableMaskedMulIOp op) : ScalableMaskedMulIOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedMulIOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedMulIOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  odsState.addTypes(res);
}

void ScalableMaskedMulIOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedMulIOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedMulIOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE6(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()))))
    return emitOpError("failed to verify that all of {src1, src2, res} have same type");
  if (!((std::equal_to<>()(getI1SameShape((*this->getODSOperands(1).begin()).getType()), (*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that mask has i1 element type and same shape as operands");
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedMulIOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult ScalableMaskedMulIOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand maskRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> maskOperands(&maskRawOperand, 1);  ::llvm::SMLoc maskOperandsLoc;
  (void)maskOperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src1Operands(&src1RawOperand, 1);  ::llvm::SMLoc src1OperandsLoc;
  (void)src1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src2Operands(&src2RawOperand, 1);  ::llvm::SMLoc src2OperandsLoc;
  (void)src2OperandsLoc;
  ::mlir::Type maskRawType{};
  ::llvm::ArrayRef<::mlir::Type> maskTypes(&maskRawType, 1);
  ::mlir::Type resRawType{};
  ::llvm::ArrayRef<::mlir::Type> resTypes(&resRawType, 1);

  maskOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(maskRawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    maskRawType = type;
  }
  if (parser.parseComma())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    resRawType = type;
  }
  result.addTypes(resTypes);
  if (parser.resolveOperands(maskOperands, maskTypes, maskOperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src1Operands, resTypes[0], src1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src2Operands, resTypes[0], src2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void ScalableMaskedMulIOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getMask();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getMask().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
  _odsPrinter << ",";
  _odsPrinter << ' ';
  {
    auto type = getRes().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedMulIOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedSDivIIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedSDivIIntrOpAdaptor::ScalableMaskedSDivIIntrOpAdaptor(ScalableMaskedSDivIIntrOp op) : ScalableMaskedSDivIIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedSDivIIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedSDivIIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  odsState.addTypes(res);
}

void ScalableMaskedSDivIIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedSDivIIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedSDivIIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedSDivIIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedSDivIIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedSDivIOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedSDivIOpAdaptor::ScalableMaskedSDivIOpAdaptor(ScalableMaskedSDivIOp op) : ScalableMaskedSDivIOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedSDivIOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedSDivIOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  odsState.addTypes(res);
}

void ScalableMaskedSDivIOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedSDivIOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedSDivIOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE6(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()))))
    return emitOpError("failed to verify that all of {src1, src2, res} have same type");
  if (!((std::equal_to<>()(getI1SameShape((*this->getODSOperands(1).begin()).getType()), (*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that mask has i1 element type and same shape as operands");
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedSDivIOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult ScalableMaskedSDivIOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand maskRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> maskOperands(&maskRawOperand, 1);  ::llvm::SMLoc maskOperandsLoc;
  (void)maskOperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src1Operands(&src1RawOperand, 1);  ::llvm::SMLoc src1OperandsLoc;
  (void)src1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src2Operands(&src2RawOperand, 1);  ::llvm::SMLoc src2OperandsLoc;
  (void)src2OperandsLoc;
  ::mlir::Type maskRawType{};
  ::llvm::ArrayRef<::mlir::Type> maskTypes(&maskRawType, 1);
  ::mlir::Type resRawType{};
  ::llvm::ArrayRef<::mlir::Type> resTypes(&resRawType, 1);

  maskOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(maskRawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    maskRawType = type;
  }
  if (parser.parseComma())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    resRawType = type;
  }
  result.addTypes(resTypes);
  if (parser.resolveOperands(maskOperands, maskTypes, maskOperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src1Operands, resTypes[0], src1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src2Operands, resTypes[0], src2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void ScalableMaskedSDivIOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getMask();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getMask().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
  _odsPrinter << ",";
  _odsPrinter << ' ';
  {
    auto type = getRes().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedSDivIOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedSubFIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedSubFIntrOpAdaptor::ScalableMaskedSubFIntrOpAdaptor(ScalableMaskedSubFIntrOp op) : ScalableMaskedSubFIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedSubFIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedSubFIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  odsState.addTypes(res);
}

void ScalableMaskedSubFIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedSubFIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedSubFIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedSubFIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedSubFIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedSubFOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedSubFOpAdaptor::ScalableMaskedSubFOpAdaptor(ScalableMaskedSubFOp op) : ScalableMaskedSubFOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedSubFOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedSubFOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  odsState.addTypes(res);
}

void ScalableMaskedSubFOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedSubFOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedSubFOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE6(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE7(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE7(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE7(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()))))
    return emitOpError("failed to verify that all of {src1, src2, res} have same type");
  if (!((std::equal_to<>()(getI1SameShape((*this->getODSOperands(1).begin()).getType()), (*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that mask has i1 element type and same shape as operands");
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedSubFOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult ScalableMaskedSubFOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand maskRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> maskOperands(&maskRawOperand, 1);  ::llvm::SMLoc maskOperandsLoc;
  (void)maskOperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src1Operands(&src1RawOperand, 1);  ::llvm::SMLoc src1OperandsLoc;
  (void)src1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src2Operands(&src2RawOperand, 1);  ::llvm::SMLoc src2OperandsLoc;
  (void)src2OperandsLoc;
  ::mlir::Type maskRawType{};
  ::llvm::ArrayRef<::mlir::Type> maskTypes(&maskRawType, 1);
  ::mlir::Type resRawType{};
  ::llvm::ArrayRef<::mlir::Type> resTypes(&resRawType, 1);

  maskOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(maskRawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    maskRawType = type;
  }
  if (parser.parseComma())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    resRawType = type;
  }
  result.addTypes(resTypes);
  if (parser.resolveOperands(maskOperands, maskTypes, maskOperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src1Operands, resTypes[0], src1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src2Operands, resTypes[0], src2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void ScalableMaskedSubFOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getMask();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getMask().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
  _odsPrinter << ",";
  _odsPrinter << ' ';
  {
    auto type = getRes().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedSubFOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedSubIIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedSubIIntrOpAdaptor::ScalableMaskedSubIIntrOpAdaptor(ScalableMaskedSubIIntrOp op) : ScalableMaskedSubIIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedSubIIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedSubIIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  odsState.addTypes(res);
}

void ScalableMaskedSubIIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedSubIIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedSubIIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedSubIIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedSubIIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedSubIOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedSubIOpAdaptor::ScalableMaskedSubIOpAdaptor(ScalableMaskedSubIOp op) : ScalableMaskedSubIOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedSubIOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedSubIOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  odsState.addTypes(res);
}

void ScalableMaskedSubIOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedSubIOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedSubIOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE6(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()))))
    return emitOpError("failed to verify that all of {src1, src2, res} have same type");
  if (!((std::equal_to<>()(getI1SameShape((*this->getODSOperands(1).begin()).getType()), (*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that mask has i1 element type and same shape as operands");
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedSubIOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult ScalableMaskedSubIOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand maskRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> maskOperands(&maskRawOperand, 1);  ::llvm::SMLoc maskOperandsLoc;
  (void)maskOperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src1Operands(&src1RawOperand, 1);  ::llvm::SMLoc src1OperandsLoc;
  (void)src1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src2Operands(&src2RawOperand, 1);  ::llvm::SMLoc src2OperandsLoc;
  (void)src2OperandsLoc;
  ::mlir::Type maskRawType{};
  ::llvm::ArrayRef<::mlir::Type> maskTypes(&maskRawType, 1);
  ::mlir::Type resRawType{};
  ::llvm::ArrayRef<::mlir::Type> resTypes(&resRawType, 1);

  maskOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(maskRawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    maskRawType = type;
  }
  if (parser.parseComma())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    resRawType = type;
  }
  result.addTypes(resTypes);
  if (parser.resolveOperands(maskOperands, maskTypes, maskOperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src1Operands, resTypes[0], src1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src2Operands, resTypes[0], src2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void ScalableMaskedSubIOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getMask();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getMask().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
  _odsPrinter << ",";
  _odsPrinter << ' ';
  {
    auto type = getRes().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedSubIOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedUDivIIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedUDivIIntrOpAdaptor::ScalableMaskedUDivIIntrOpAdaptor(ScalableMaskedUDivIIntrOp op) : ScalableMaskedUDivIIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedUDivIIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedUDivIIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  odsState.addTypes(res);
}

void ScalableMaskedUDivIIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedUDivIIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedUDivIIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedUDivIIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedUDivIIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ScalableMaskedUDivIOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ScalableMaskedUDivIOpAdaptor::ScalableMaskedUDivIOpAdaptor(ScalableMaskedUDivIOp op) : ScalableMaskedUDivIOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ScalableMaskedUDivIOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ScalableMaskedUDivIOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  odsState.addTypes(res);
}

void ScalableMaskedUDivIOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value mask, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(mask);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ScalableMaskedUDivIOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ScalableMaskedUDivIOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE6(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE8(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()))))
    return emitOpError("failed to verify that all of {src1, src2, res} have same type");
  if (!((std::equal_to<>()(getI1SameShape((*this->getODSOperands(1).begin()).getType()), (*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that mask has i1 element type and same shape as operands");
  return ::mlir::success();
}

::mlir::LogicalResult ScalableMaskedUDivIOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult ScalableMaskedUDivIOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand maskRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> maskOperands(&maskRawOperand, 1);  ::llvm::SMLoc maskOperandsLoc;
  (void)maskOperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src1Operands(&src1RawOperand, 1);  ::llvm::SMLoc src1OperandsLoc;
  (void)src1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src2Operands(&src2RawOperand, 1);  ::llvm::SMLoc src2OperandsLoc;
  (void)src2OperandsLoc;
  ::mlir::Type maskRawType{};
  ::llvm::ArrayRef<::mlir::Type> maskTypes(&maskRawType, 1);
  ::mlir::Type resRawType{};
  ::llvm::ArrayRef<::mlir::Type> resTypes(&resRawType, 1);

  maskOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(maskRawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    maskRawType = type;
  }
  if (parser.parseComma())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    resRawType = type;
  }
  result.addTypes(resTypes);
  if (parser.resolveOperands(maskOperands, maskTypes, maskOperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src1Operands, resTypes[0], src1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src2Operands, resTypes[0], src2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void ScalableMaskedUDivIOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getMask();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getMask().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
  _odsPrinter << ",";
  _odsPrinter << ' ';
  {
    auto type = getRes().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ScalableMaskedUDivIOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::SdotIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
SdotIntrOpAdaptor::SdotIntrOpAdaptor(SdotIntrOp op) : SdotIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult SdotIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void SdotIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  odsState.addTypes(res);
}

void SdotIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void SdotIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult SdotIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult SdotIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::SdotIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::SdotOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
SdotOpAdaptor::SdotOpAdaptor(SdotOp op) : SdotOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult SdotOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void SdotOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type dst, ::mlir::Value acc, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(acc);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  odsState.addTypes(dst);
}

void SdotOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value acc, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(acc);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void SdotOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult SdotOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE9(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE10(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE10(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE9(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()))))
    return emitOpError("failed to verify that all of {src1, src2} have same type");
  if (!((((*this->getODSOperands(0).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that all of {acc, dst} have same type");
  return ::mlir::success();
}

::mlir::LogicalResult SdotOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult SdotOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand accRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> accOperands(&accRawOperand, 1);  ::llvm::SMLoc accOperandsLoc;
  (void)accOperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src1Operands(&src1RawOperand, 1);  ::llvm::SMLoc src1OperandsLoc;
  (void)src1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src2Operands(&src2RawOperand, 1);  ::llvm::SMLoc src2OperandsLoc;
  (void)src2OperandsLoc;
  ::mlir::Type src1RawType{};
  ::llvm::ArrayRef<::mlir::Type> src1Types(&src1RawType, 1);
  ::mlir::Type dstRawType{};
  ::llvm::ArrayRef<::mlir::Type> dstTypes(&dstRawType, 1);

  accOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(accRawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    src1RawType = type;
  }
  if (parser.parseKeyword("to"))
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    dstRawType = type;
  }
  result.addTypes(dstTypes);
  if (parser.resolveOperands(accOperands, dstTypes[0], accOperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src1Operands, src1Types, src1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src2Operands, src1Types[0], src2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void SdotOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getAcc();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getSrc1().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
  _odsPrinter << ' ' << "to";
  _odsPrinter << ' ';
  {
    auto type = getDst().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

void SdotOp::getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects) {
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::SdotOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::SmmlaIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
SmmlaIntrOpAdaptor::SmmlaIntrOpAdaptor(SmmlaIntrOp op) : SmmlaIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult SmmlaIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void SmmlaIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  odsState.addTypes(res);
}

void SmmlaIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void SmmlaIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult SmmlaIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult SmmlaIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::SmmlaIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::SmmlaOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
SmmlaOpAdaptor::SmmlaOpAdaptor(SmmlaOp op) : SmmlaOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult SmmlaOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void SmmlaOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type dst, ::mlir::Value acc, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(acc);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  odsState.addTypes(dst);
}

void SmmlaOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value acc, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(acc);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void SmmlaOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult SmmlaOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE11(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE12(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE12(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE11(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()))))
    return emitOpError("failed to verify that all of {src1, src2} have same type");
  if (!((((*this->getODSOperands(0).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that all of {acc, dst} have same type");
  return ::mlir::success();
}

::mlir::LogicalResult SmmlaOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult SmmlaOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand accRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> accOperands(&accRawOperand, 1);  ::llvm::SMLoc accOperandsLoc;
  (void)accOperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src1Operands(&src1RawOperand, 1);  ::llvm::SMLoc src1OperandsLoc;
  (void)src1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src2Operands(&src2RawOperand, 1);  ::llvm::SMLoc src2OperandsLoc;
  (void)src2OperandsLoc;
  ::mlir::Type src1RawType{};
  ::llvm::ArrayRef<::mlir::Type> src1Types(&src1RawType, 1);
  ::mlir::Type dstRawType{};
  ::llvm::ArrayRef<::mlir::Type> dstTypes(&dstRawType, 1);

  accOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(accRawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    src1RawType = type;
  }
  if (parser.parseKeyword("to"))
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    dstRawType = type;
  }
  result.addTypes(dstTypes);
  if (parser.resolveOperands(accOperands, dstTypes[0], accOperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src1Operands, src1Types, src1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src2Operands, src1Types[0], src2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void SmmlaOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getAcc();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getSrc1().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
  _odsPrinter << ' ' << "to";
  _odsPrinter << ' ';
  {
    auto type = getDst().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

void SmmlaOp::getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects) {
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::SmmlaOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::UdotIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
UdotIntrOpAdaptor::UdotIntrOpAdaptor(UdotIntrOp op) : UdotIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult UdotIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void UdotIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  odsState.addTypes(res);
}

void UdotIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void UdotIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult UdotIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult UdotIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::UdotIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::UdotOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
UdotOpAdaptor::UdotOpAdaptor(UdotOp op) : UdotOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult UdotOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void UdotOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type dst, ::mlir::Value acc, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(acc);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  odsState.addTypes(dst);
}

void UdotOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value acc, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(acc);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void UdotOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult UdotOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE9(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE10(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE10(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE9(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()))))
    return emitOpError("failed to verify that all of {src1, src2} have same type");
  if (!((((*this->getODSOperands(0).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that all of {acc, dst} have same type");
  return ::mlir::success();
}

::mlir::LogicalResult UdotOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult UdotOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand accRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> accOperands(&accRawOperand, 1);  ::llvm::SMLoc accOperandsLoc;
  (void)accOperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src1Operands(&src1RawOperand, 1);  ::llvm::SMLoc src1OperandsLoc;
  (void)src1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src2Operands(&src2RawOperand, 1);  ::llvm::SMLoc src2OperandsLoc;
  (void)src2OperandsLoc;
  ::mlir::Type src1RawType{};
  ::llvm::ArrayRef<::mlir::Type> src1Types(&src1RawType, 1);
  ::mlir::Type dstRawType{};
  ::llvm::ArrayRef<::mlir::Type> dstTypes(&dstRawType, 1);

  accOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(accRawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    src1RawType = type;
  }
  if (parser.parseKeyword("to"))
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    dstRawType = type;
  }
  result.addTypes(dstTypes);
  if (parser.resolveOperands(accOperands, dstTypes[0], accOperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src1Operands, src1Types, src1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src2Operands, src1Types[0], src2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void UdotOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getAcc();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getSrc1().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
  _odsPrinter << ' ' << "to";
  _odsPrinter << ' ';
  {
    auto type = getDst().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

void UdotOp::getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects) {
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::UdotOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::UmmlaIntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
UmmlaIntrOpAdaptor::UmmlaIntrOpAdaptor(UmmlaIntrOp op) : UmmlaIntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult UmmlaIntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void UmmlaIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  odsState.addTypes(res);
}

void UmmlaIntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value odsArg_0, ::mlir::Value odsArg_1, ::mlir::Value odsArg_2) {
  odsState.addOperands(odsArg_0);
  odsState.addOperands(odsArg_1);
  odsState.addOperands(odsArg_2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void UmmlaIntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult UmmlaIntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult UmmlaIntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::UmmlaIntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::UmmlaOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
UmmlaOpAdaptor::UmmlaOpAdaptor(UmmlaOp op) : UmmlaOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult UmmlaOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void UmmlaOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type dst, ::mlir::Value acc, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(acc);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  odsState.addTypes(dst);
}

void UmmlaOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value acc, ::mlir::Value src1, ::mlir::Value src2) {
  odsState.addOperands(acc);
  odsState.addOperands(src1);
  odsState.addOperands(src2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void UmmlaOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 3u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult UmmlaOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE11(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE12(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE12(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE11(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()))))
    return emitOpError("failed to verify that all of {src1, src2} have same type");
  if (!((((*this->getODSOperands(0).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that all of {acc, dst} have same type");
  return ::mlir::success();
}

::mlir::LogicalResult UmmlaOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult UmmlaOp::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand accRawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> accOperands(&accRawOperand, 1);  ::llvm::SMLoc accOperandsLoc;
  (void)accOperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src1Operands(&src1RawOperand, 1);  ::llvm::SMLoc src1OperandsLoc;
  (void)src1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand src2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> src2Operands(&src2RawOperand, 1);  ::llvm::SMLoc src2OperandsLoc;
  (void)src2OperandsLoc;
  ::mlir::Type src1RawType{};
  ::llvm::ArrayRef<::mlir::Type> src1Types(&src1RawType, 1);
  ::mlir::Type dstRawType{};
  ::llvm::ArrayRef<::mlir::Type> dstTypes(&dstRawType, 1);

  accOperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(accRawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  src2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(src2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    src1RawType = type;
  }
  if (parser.parseKeyword("to"))
    return ::mlir::failure();

  {
    ::mlir::VectorType type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    dstRawType = type;
  }
  result.addTypes(dstTypes);
  if (parser.resolveOperands(accOperands, dstTypes[0], accOperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src1Operands, src1Types, src1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(src2Operands, src1Types[0], src2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void UmmlaOp::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getAcc();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSrc2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getSrc1().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
  _odsPrinter << ' ' << "to";
  _odsPrinter << ' ';
  {
    auto type = getDst().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::VectorType>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

void UmmlaOp::getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects) {
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::UmmlaOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ZipX2IntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ZipX2IntrOpAdaptor::ZipX2IntrOpAdaptor(ZipX2IntrOp op) : ZipX2IntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ZipX2IntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ZipX2IntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value v1, ::mlir::Value v2) {
  odsState.addOperands(v1);
  odsState.addOperands(v2);
  odsState.addTypes(res);
}

void ZipX2IntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value v1, ::mlir::Value v2) {
  odsState.addOperands(v1);
  odsState.addOperands(v2);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ZipX2IntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 2u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ZipX2IntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult ZipX2IntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ZipX2IntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ZipX2Op definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ZipX2OpAdaptor::ZipX2OpAdaptor(ZipX2Op op) : ZipX2OpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ZipX2OpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ZipX2Op::getAsmResultNames(::mlir::OpAsmSetValueNameFn setNameFn) {
  auto resultGroup0 = getODSResults(0);
  if (!resultGroup0.empty())
    setNameFn(*resultGroup0.begin(), "resultV1");
  auto resultGroup1 = getODSResults(1);
  if (!resultGroup1.empty())
    setNameFn(*resultGroup1.begin(), "resultV2");
}

void ZipX2Op::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, Value v1, Value v2) {
      build(odsBuilder, odsState, v1.getType(), v1.getType(), v1, v2);
  
}

void ZipX2Op::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type resultV1, ::mlir::Type resultV2, ::mlir::Value sourceV1, ::mlir::Value sourceV2) {
  odsState.addOperands(sourceV1);
  odsState.addOperands(sourceV2);
  odsState.addTypes(resultV1);
  odsState.addTypes(resultV2);
}

void ZipX2Op::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value sourceV1, ::mlir::Value sourceV2) {
  odsState.addOperands(sourceV1);
  odsState.addOperands(sourceV2);
  assert(resultTypes.size() == 2u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ZipX2Op::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 2u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 2u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ZipX2Op::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE13(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE13(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE13(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSResults(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE13(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(0).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()) && ((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSResults(1).begin()).getType()) && ((*this->getODSResults(1).begin()).getType()) == ((*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that all of {sourceV1, sourceV2, resultV1, resultV2} have same type");
  return ::mlir::success();
}

::mlir::LogicalResult ZipX2Op::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult ZipX2Op::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand sourceV1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> sourceV1Operands(&sourceV1RawOperand, 1);  ::llvm::SMLoc sourceV1OperandsLoc;
  (void)sourceV1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand sourceV2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> sourceV2Operands(&sourceV2RawOperand, 1);  ::llvm::SMLoc sourceV2OperandsLoc;
  (void)sourceV2OperandsLoc;
  ::mlir::Type sourceV1RawType{};
  ::llvm::ArrayRef<::mlir::Type> sourceV1Types(&sourceV1RawType, 1);

  sourceV1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(sourceV1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  sourceV2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(sourceV2RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::Type type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    sourceV1RawType = type;
  }
  result.addTypes(sourceV1Types[0]);
  result.addTypes(sourceV1Types[0]);
  if (parser.resolveOperands(sourceV1Operands, sourceV1Types, sourceV1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(sourceV2Operands, sourceV1Types[0], sourceV2OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void ZipX2Op::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getSourceV1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSourceV2();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getSourceV1().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::Type>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

void ZipX2Op::getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects) {
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ZipX2Op)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ZipX4IntrOp definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ZipX4IntrOpAdaptor::ZipX4IntrOpAdaptor(ZipX4IntrOp op) : ZipX4IntrOpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ZipX4IntrOpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ZipX4IntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type res, ::mlir::Value v1, ::mlir::Value v2, ::mlir::Value v3, ::mlir::Value v4) {
  odsState.addOperands(v1);
  odsState.addOperands(v2);
  odsState.addOperands(v3);
  odsState.addOperands(v4);
  odsState.addTypes(res);
}

void ZipX4IntrOp::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value v1, ::mlir::Value v2, ::mlir::Value v3, ::mlir::Value v4) {
  odsState.addOperands(v1);
  odsState.addOperands(v2);
  odsState.addOperands(v3);
  odsState.addOperands(v4);
  assert(resultTypes.size() == 1u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ZipX4IntrOp::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 4u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 1u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ZipX4IntrOp::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup3 = getODSOperands(3);

    for (auto v : valueGroup3) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE5(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE1(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  return ::mlir::success();
}

::mlir::LogicalResult ZipX4IntrOp::verifyInvariants() {
  return verifyInvariantsImpl();
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ZipX4IntrOp)

namespace mlir {
namespace arm_sve {

//===----------------------------------------------------------------------===//
// ::mlir::arm_sve::ZipX4Op definitions
//===----------------------------------------------------------------------===//

namespace detail {
} // namespace detail
ZipX4OpAdaptor::ZipX4OpAdaptor(ZipX4Op op) : ZipX4OpGenericAdaptor(op->getOperands(), op) {}

::mlir::LogicalResult ZipX4OpAdaptor::verify(::mlir::Location loc) {
  return ::mlir::success();
}

void ZipX4Op::getAsmResultNames(::mlir::OpAsmSetValueNameFn setNameFn) {
  auto resultGroup0 = getODSResults(0);
  if (!resultGroup0.empty())
    setNameFn(*resultGroup0.begin(), "resultV1");
  auto resultGroup1 = getODSResults(1);
  if (!resultGroup1.empty())
    setNameFn(*resultGroup1.begin(), "resultV2");
  auto resultGroup2 = getODSResults(2);
  if (!resultGroup2.empty())
    setNameFn(*resultGroup2.begin(), "resultV3");
  auto resultGroup3 = getODSResults(3);
  if (!resultGroup3.empty())
    setNameFn(*resultGroup3.begin(), "resultV4");
}

void ZipX4Op::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, Value v1, Value v2, Value v3, Value v4) {
      build(odsBuilder, odsState,
        v1.getType(), v1.getType(),
        v1.getType(), v1.getType(),
        v1, v2, v3, v4);
  
}

void ZipX4Op::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type resultV1, ::mlir::Type resultV2, ::mlir::Type resultV3, ::mlir::Type resultV4, ::mlir::Value sourceV1, ::mlir::Value sourceV2, ::mlir::Value sourceV3, ::mlir::Value sourceV4) {
  odsState.addOperands(sourceV1);
  odsState.addOperands(sourceV2);
  odsState.addOperands(sourceV3);
  odsState.addOperands(sourceV4);
  odsState.addTypes(resultV1);
  odsState.addTypes(resultV2);
  odsState.addTypes(resultV3);
  odsState.addTypes(resultV4);
}

void ZipX4Op::build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value sourceV1, ::mlir::Value sourceV2, ::mlir::Value sourceV3, ::mlir::Value sourceV4) {
  odsState.addOperands(sourceV1);
  odsState.addOperands(sourceV2);
  odsState.addOperands(sourceV3);
  odsState.addOperands(sourceV4);
  assert(resultTypes.size() == 4u && "mismatched number of results");
  odsState.addTypes(resultTypes);
}

void ZipX4Op::build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes) {
  assert(operands.size() == 4u && "mismatched number of parameters");
  odsState.addOperands(operands);
  odsState.addAttributes(attributes);
  assert(resultTypes.size() == 4u && "mismatched number of return types");
  odsState.addTypes(resultTypes);
}

::mlir::LogicalResult ZipX4Op::verifyInvariantsImpl() {
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSOperands(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE13(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSOperands(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE13(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSOperands(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE13(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
    auto valueGroup3 = getODSOperands(3);

    for (auto v : valueGroup3) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE13(*this, v.getType(), "operand", index++)))
        return ::mlir::failure();
    }
  }
  {
    unsigned index = 0; (void)index;
    auto valueGroup0 = getODSResults(0);

    for (auto v : valueGroup0) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE13(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
    auto valueGroup1 = getODSResults(1);

    for (auto v : valueGroup1) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE13(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
    auto valueGroup2 = getODSResults(2);

    for (auto v : valueGroup2) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE13(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
    auto valueGroup3 = getODSResults(3);

    for (auto v : valueGroup3) {
      if (::mlir::failed(__mlir_ods_local_type_constraint_ArmSVE13(*this, v.getType(), "result", index++)))
        return ::mlir::failure();
    }
  }
  if (!((((*this->getODSOperands(0).begin()).getType()) == ((*this->getODSOperands(1).begin()).getType()) && ((*this->getODSOperands(1).begin()).getType()) == ((*this->getODSOperands(2).begin()).getType()) && ((*this->getODSOperands(2).begin()).getType()) == ((*this->getODSOperands(3).begin()).getType()) && ((*this->getODSOperands(3).begin()).getType()) == ((*this->getODSResults(0).begin()).getType()) && ((*this->getODSResults(0).begin()).getType()) == ((*this->getODSResults(1).begin()).getType()) && ((*this->getODSResults(1).begin()).getType()) == ((*this->getODSResults(2).begin()).getType()) && ((*this->getODSResults(2).begin()).getType()) == ((*this->getODSResults(3).begin()).getType()) && ((*this->getODSResults(3).begin()).getType()) == ((*this->getODSOperands(0).begin()).getType()))))
    return emitOpError("failed to verify that all of {sourceV1, sourceV2, sourceV3, sourceV4, resultV1, resultV2, resultV3, resultV4} have same type");
  return ::mlir::success();
}

::mlir::LogicalResult ZipX4Op::verifyInvariants() {
  return verifyInvariantsImpl();
}

::mlir::ParseResult ZipX4Op::parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result) {
  ::mlir::OpAsmParser::UnresolvedOperand sourceV1RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> sourceV1Operands(&sourceV1RawOperand, 1);  ::llvm::SMLoc sourceV1OperandsLoc;
  (void)sourceV1OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand sourceV2RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> sourceV2Operands(&sourceV2RawOperand, 1);  ::llvm::SMLoc sourceV2OperandsLoc;
  (void)sourceV2OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand sourceV3RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> sourceV3Operands(&sourceV3RawOperand, 1);  ::llvm::SMLoc sourceV3OperandsLoc;
  (void)sourceV3OperandsLoc;
  ::mlir::OpAsmParser::UnresolvedOperand sourceV4RawOperand{};
  ::llvm::ArrayRef<::mlir::OpAsmParser::UnresolvedOperand> sourceV4Operands(&sourceV4RawOperand, 1);  ::llvm::SMLoc sourceV4OperandsLoc;
  (void)sourceV4OperandsLoc;
  ::mlir::Type sourceV1RawType{};
  ::llvm::ArrayRef<::mlir::Type> sourceV1Types(&sourceV1RawType, 1);

  sourceV1OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(sourceV1RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  sourceV2OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(sourceV2RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  sourceV3OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(sourceV3RawOperand))
    return ::mlir::failure();
  if (parser.parseComma())
    return ::mlir::failure();

  sourceV4OperandsLoc = parser.getCurrentLocation();
  if (parser.parseOperand(sourceV4RawOperand))
    return ::mlir::failure();
  {
    auto loc = parser.getCurrentLocation();(void)loc;
    if (parser.parseOptionalAttrDict(result.attributes))
      return ::mlir::failure();
  }
  if (parser.parseColon())
    return ::mlir::failure();

  {
    ::mlir::Type type;
    if (parser.parseCustomTypeWithFallback(type))
      return ::mlir::failure();
    sourceV1RawType = type;
  }
  result.addTypes(sourceV1Types[0]);
  result.addTypes(sourceV1Types[0]);
  result.addTypes(sourceV1Types[0]);
  result.addTypes(sourceV1Types[0]);
  if (parser.resolveOperands(sourceV1Operands, sourceV1Types, sourceV1OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(sourceV2Operands, sourceV1Types[0], sourceV2OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(sourceV3Operands, sourceV1Types[0], sourceV3OperandsLoc, result.operands))
    return ::mlir::failure();
  if (parser.resolveOperands(sourceV4Operands, sourceV1Types[0], sourceV4OperandsLoc, result.operands))
    return ::mlir::failure();
  return ::mlir::success();
}

void ZipX4Op::print(::mlir::OpAsmPrinter &_odsPrinter) {
  _odsPrinter << ' ';
  _odsPrinter << getSourceV1();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSourceV2();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSourceV3();
  _odsPrinter << ",";
  _odsPrinter << ' ';
  _odsPrinter << getSourceV4();
  ::llvm::SmallVector<::llvm::StringRef, 2> elidedAttrs;
  _odsPrinter.printOptionalAttrDict((*this)->getAttrs(), elidedAttrs);
  _odsPrinter << ' ' << ":";
  _odsPrinter << ' ';
  {
    auto type = getSourceV1().getType();
    if (auto validType = ::llvm::dyn_cast<::mlir::Type>(type))
      _odsPrinter.printStrippedAttrOrType(validType);
   else
     _odsPrinter << type;
  }
}

void ZipX4Op::getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects) {
}

} // namespace arm_sve
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::arm_sve::ZipX4Op)


#endif  // GET_OP_CLASSES

