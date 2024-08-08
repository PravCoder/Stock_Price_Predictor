/*===- TableGen'erated file -------------------------------------*- C++ -*-===*\
|*                                                                            *|
|* Op Declarations                                                            *|
|*                                                                            *|
|* Automatically generated file, do not edit!                                 *|
|* From: tfr_ops.td                                                           *|
|*                                                                            *|
\*===----------------------------------------------------------------------===*/

namespace mlir {
namespace TFR {
class BuildListOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class CallOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class CastOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class ConstOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class ConstantTensorOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class EqualOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class GetElementOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class GetElementTypeOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class GetLengthOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class GetShapeOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class TFRFuncOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class TFRQuantActRangeOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class TFRQuantQParamsOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class TFRQuantRawDataOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class TFRQuantRescaleOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class TFRQuantScaleFactorOp;
} // namespace TFR
} // namespace mlir
namespace mlir {
namespace TFR {
class TFRReturnOp;
} // namespace TFR
} // namespace mlir
#ifdef GET_OP_CLASSES
#undef GET_OP_CLASSES

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::BuildListOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class BuildListOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  BuildListOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.build_list", odsAttrs.getContext());
  }

  BuildListOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize);
  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class BuildListOpGenericAdaptor : public detail::BuildListOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::BuildListOpGenericAdaptorBase;
public:
  BuildListOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  BuildListOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : BuildListOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = BuildListOp, typename = std::enable_if_t<std::is_same_v<LateInst, BuildListOp>>>
  BuildListOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  RangeT getTensors() {
    return getODSOperands(0);
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class BuildListOpAdaptor : public BuildListOpGenericAdaptor<::mlir::ValueRange> {
public:
  using BuildListOpGenericAdaptor::BuildListOpGenericAdaptor;
  BuildListOpAdaptor(BuildListOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class BuildListOp : public ::mlir::Op<BuildListOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::VariadicOperands, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = BuildListOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = BuildListOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.build_list");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index);
  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Operation::operand_range getTensors() {
    return getODSOperands(0);
  }

  ::mlir::MutableOperandRange getTensorsMutable();
  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getOut() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(0).begin());
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type out, ::mlir::ValueRange tensors);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static void getCanonicalizationPatterns(::mlir::RewritePatternSet &results, ::mlir::MLIRContext *context);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::BuildListOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::CallOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class CallOpGenericAdaptorBase {
public:
  struct Properties {
    using calleeTy = ::mlir::FlatSymbolRefAttr;
    calleeTy callee;

    auto getCallee() {
      auto &propStorage = this->callee;
      return ::llvm::cast<::mlir::FlatSymbolRefAttr>(propStorage);
    }
    void setCallee(const ::mlir::FlatSymbolRefAttr &propValue) {
      this->callee = propValue;
    }
    bool operator==(const Properties &rhs) const {
      return 
        rhs.callee == this->callee &&
        true;
    }
    bool operator!=(const Properties &rhs) const {
      return !(*this == rhs);
    }
  };
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  Properties properties;
  ::mlir::RegionRange odsRegions;
public:
  CallOpGenericAdaptorBase(::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), properties(properties), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.call", odsAttrs.getContext());
  }

  CallOpGenericAdaptorBase(CallOp op);

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize);
  const Properties &getProperties() {
    return properties;
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

  ::mlir::FlatSymbolRefAttr getCalleeAttr() {
    auto attr = ::llvm::cast<::mlir::FlatSymbolRefAttr>(getProperties().callee);
    return attr;
  }

  ::llvm::StringRef getCallee();
};
} // namespace detail
template <typename RangeT>
class CallOpGenericAdaptor : public detail::CallOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::CallOpGenericAdaptorBase;
public:
  CallOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  CallOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : CallOpGenericAdaptor(values, attrs, (properties ? *properties.as<Properties *>() : Properties{}), regions) {}

  CallOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = nullptr) : CallOpGenericAdaptor(values, attrs, Properties{}, {}) {}

  template <typename LateInst = CallOp, typename = std::enable_if_t<std::is_same_v<LateInst, CallOp>>>
  CallOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  RangeT getArgs() {
    return getODSOperands(0);
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class CallOpAdaptor : public CallOpGenericAdaptor<::mlir::ValueRange> {
public:
  using CallOpGenericAdaptor::CallOpGenericAdaptor;
  CallOpAdaptor(CallOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class CallOp : public ::mlir::Op<CallOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::VariadicResults, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::VariadicOperands, ::mlir::OpTrait::OpInvariants, ::mlir::BytecodeOpInterface::Trait, ::mlir::CallOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = CallOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = CallOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  using Properties = FoldAdaptor::Properties;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    static ::llvm::StringRef attrNames[] = {::llvm::StringRef("callee")};
    return ::llvm::ArrayRef(attrNames);
  }

  ::mlir::StringAttr getCalleeAttrName() {
    return getAttributeNameForIndex(0);
  }

  static ::mlir::StringAttr getCalleeAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 0);
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.call");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index);
  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Operation::operand_range getArgs() {
    return getODSOperands(0);
  }

  ::mlir::MutableOperandRange getArgsMutable();
  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index);
  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Operation::result_range getOuts() {
    return getODSResults(0);
  }

  static ::mlir::LogicalResult setPropertiesFromAttr(Properties &prop, ::mlir::Attribute attr, ::llvm::function_ref<::mlir::InFlightDiagnostic()> emitError);
  static ::mlir::Attribute getPropertiesAsAttr(::mlir::MLIRContext *ctx, const Properties &prop);
  static llvm::hash_code computePropertiesHash(const Properties &prop);
  static std::optional<mlir::Attribute> getInherentAttr(::mlir::MLIRContext *ctx, const Properties &prop, llvm::StringRef name);
  static void setInherentAttr(Properties &prop, llvm::StringRef name, mlir::Attribute value);
  static void populateInherentAttrs(::mlir::MLIRContext *ctx, const Properties &prop, ::mlir::NamedAttrList &attrs);
  static ::mlir::LogicalResult verifyInherentAttrs(::mlir::OperationName opName, ::mlir::NamedAttrList &attrs, llvm::function_ref<::mlir::InFlightDiagnostic()> emitError);
  static ::mlir::LogicalResult readProperties(::mlir::DialectBytecodeReader &reader, ::mlir::OperationState &state);
  void writeProperties(::mlir::DialectBytecodeWriter &writer);
  ::mlir::FlatSymbolRefAttr getCalleeAttr() {
    return ::llvm::cast<::mlir::FlatSymbolRefAttr>(getProperties().callee);
  }

  ::llvm::StringRef getCallee();
  void setCalleeAttr(::mlir::FlatSymbolRefAttr attr) {
    getProperties().callee = attr;
  }

  void setCallee(::llvm::StringRef attrValue);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange outs, ::mlir::FlatSymbolRefAttr callee, ::mlir::ValueRange args);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange outs, ::llvm::StringRef callee, ::mlir::ValueRange args);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &_odsPrinter);
private:
  ::mlir::StringAttr getAttributeNameForIndex(unsigned index) {
    return getAttributeNameForIndex((*this)->getName(), index);
  }

  static ::mlir::StringAttr getAttributeNameForIndex(::mlir::OperationName name, unsigned index) {
    assert(index < 1 && "invalid attribute index");
    assert(name.getStringRef() == getOperationName() && "invalid operation name");
    assert(name.isRegistered() && "Operation isn't registered, missing a "
          "dependent dialect loading?");
    return name.getAttributeNames()[index];
  }

public:
  // Get the argument operands to the called function.
  operand_range getArgOperands() { return getArgs(); }
  MutableOperandRange getArgOperandsMutable() {
    return getArgsMutable();
  }

  // Return the callee of this operation.
  CallInterfaceCallable getCallableForCallee() { return getCalleeAttr(); }
  // Sets the callee from the callable
  void setCalleeFromCallable(CallInterfaceCallable callee);
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::CallOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::CastOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class CastOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  CastOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.cast", odsAttrs.getContext());
  }

  CastOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class CastOpGenericAdaptor : public detail::CastOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::CastOpGenericAdaptorBase;
public:
  CastOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  CastOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : CastOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = CastOp, typename = std::enable_if_t<std::is_same_v<LateInst, CastOp>>>
  CastOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getArg() {
    return (*getODSOperands(0).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class CastOpAdaptor : public CastOpGenericAdaptor<::mlir::ValueRange> {
public:
  using CastOpGenericAdaptor::CastOpGenericAdaptor;
  CastOpAdaptor(CastOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class CastOp : public ::mlir::Op<CastOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = CastOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = CastOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.cast");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getArg() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(0).begin());
  }

  ::mlir::OpOperand &getArgMutable() {
    auto range = getODSOperandIndexAndLength(0);
    return getOperation()->getOpOperand(range.first);
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getOut() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(0).begin());
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type out, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static void getCanonicalizationPatterns(::mlir::RewritePatternSet &results, ::mlir::MLIRContext *context);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
  // Return element type of the input tensor type. Only available when the
  // input is a MLIR built-in tensor type.
  Attribute getInputElementType() {
    if (auto ty = getArg().getType().dyn_cast<TensorType>()) {
      return TypeAttr::get(ty.getElementType());
    }
    return {};
  }
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::CastOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::ConstOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class ConstOpGenericAdaptorBase {
public:
  struct Properties {
    using valueTy = Attribute;
    valueTy value;

    auto getValue() {
      auto &propStorage = this->value;
      return ::llvm::cast<Attribute>(propStorage);
    }
    void setValue(const Attribute &propValue) {
      this->value = propValue;
    }
    bool operator==(const Properties &rhs) const {
      return 
        rhs.value == this->value &&
        true;
    }
    bool operator!=(const Properties &rhs) const {
      return !(*this == rhs);
    }
  };
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  Properties properties;
  ::mlir::RegionRange odsRegions;
public:
  ConstOpGenericAdaptorBase(::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), properties(properties), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.constant", odsAttrs.getContext());
  }

  ConstOpGenericAdaptorBase(ConstOp op);

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  const Properties &getProperties() {
    return properties;
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

  Attribute getValueAttr() {
    auto attr = ::llvm::cast<Attribute>(getProperties().value);
    return attr;
  }

  Attribute getValue();
};
} // namespace detail
template <typename RangeT>
class ConstOpGenericAdaptor : public detail::ConstOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::ConstOpGenericAdaptorBase;
public:
  ConstOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  ConstOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : ConstOpGenericAdaptor(values, attrs, (properties ? *properties.as<Properties *>() : Properties{}), regions) {}

  ConstOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = nullptr) : ConstOpGenericAdaptor(values, attrs, Properties{}, {}) {}

  template <typename LateInst = ConstOp, typename = std::enable_if_t<std::is_same_v<LateInst, ConstOp>>>
  ConstOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class ConstOpAdaptor : public ConstOpGenericAdaptor<::mlir::ValueRange> {
public:
  using ConstOpGenericAdaptor::ConstOpGenericAdaptor;
  ConstOpAdaptor(ConstOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class ConstOp : public ::mlir::Op<ConstOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::ZeroOperands, ::mlir::OpTrait::OpInvariants, ::mlir::BytecodeOpInterface::Trait, ::mlir::OpTrait::ConstantLike, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::InferTypeOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = ConstOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = ConstOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  using Properties = FoldAdaptor::Properties;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    static ::llvm::StringRef attrNames[] = {::llvm::StringRef("value")};
    return ::llvm::ArrayRef(attrNames);
  }

  ::mlir::StringAttr getValueAttrName() {
    return getAttributeNameForIndex(0);
  }

  static ::mlir::StringAttr getValueAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 0);
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.constant");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getOut() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(0).begin());
  }

  static ::mlir::LogicalResult setPropertiesFromAttr(Properties &prop, ::mlir::Attribute attr, ::llvm::function_ref<::mlir::InFlightDiagnostic()> emitError);
  static ::mlir::Attribute getPropertiesAsAttr(::mlir::MLIRContext *ctx, const Properties &prop);
  static llvm::hash_code computePropertiesHash(const Properties &prop);
  static std::optional<mlir::Attribute> getInherentAttr(::mlir::MLIRContext *ctx, const Properties &prop, llvm::StringRef name);
  static void setInherentAttr(Properties &prop, llvm::StringRef name, mlir::Attribute value);
  static void populateInherentAttrs(::mlir::MLIRContext *ctx, const Properties &prop, ::mlir::NamedAttrList &attrs);
  static ::mlir::LogicalResult verifyInherentAttrs(::mlir::OperationName opName, ::mlir::NamedAttrList &attrs, llvm::function_ref<::mlir::InFlightDiagnostic()> emitError);
  static ::mlir::LogicalResult readProperties(::mlir::DialectBytecodeReader &reader, ::mlir::OperationState &state);
  void writeProperties(::mlir::DialectBytecodeWriter &writer);
  Attribute getValueAttr() {
    return ::llvm::cast<Attribute>(getProperties().value);
  }

  Attribute getValue();
  void setValueAttr(Attribute attr) {
    getProperties().value = attr;
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, Attribute value);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type out, Attribute value);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, Attribute value);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  ::mlir::OpFoldResult fold(FoldAdaptor adaptor);
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &_odsPrinter);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
private:
  ::mlir::StringAttr getAttributeNameForIndex(unsigned index) {
    return getAttributeNameForIndex((*this)->getName(), index);
  }

  static ::mlir::StringAttr getAttributeNameForIndex(::mlir::OperationName name, unsigned index) {
    assert(index < 1 && "invalid attribute index");
    assert(name.getStringRef() == getOperationName() && "invalid operation name");
    assert(name.isRegistered() && "Operation isn't registered, missing a "
          "dependent dialect loading?");
    return name.getAttributeNames()[index];
  }

public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::ConstOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::ConstantTensorOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class ConstantTensorOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  ConstantTensorOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.constant_tensor", odsAttrs.getContext());
  }

  ConstantTensorOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class ConstantTensorOpGenericAdaptor : public detail::ConstantTensorOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::ConstantTensorOpGenericAdaptorBase;
public:
  ConstantTensorOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  ConstantTensorOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : ConstantTensorOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = ConstantTensorOp, typename = std::enable_if_t<std::is_same_v<LateInst, ConstantTensorOp>>>
  ConstantTensorOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getArg() {
    return (*getODSOperands(0).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class ConstantTensorOpAdaptor : public ConstantTensorOpGenericAdaptor<::mlir::ValueRange> {
public:
  using ConstantTensorOpGenericAdaptor::ConstantTensorOpGenericAdaptor;
  ConstantTensorOpAdaptor(ConstantTensorOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class ConstantTensorOp : public ::mlir::Op<ConstantTensorOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = ConstantTensorOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = ConstantTensorOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.constant_tensor");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getArg() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(0).begin());
  }

  ::mlir::OpOperand &getArgMutable() {
    auto range = getODSOperandIndexAndLength(0);
    return getOperation()->getOpOperand(range.first);
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getOut() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(0).begin());
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type out, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  ::mlir::LogicalResult verify();
  static void getCanonicalizationPatterns(::mlir::RewritePatternSet &results, ::mlir::MLIRContext *context);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::ConstantTensorOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::EqualOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class EqualOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  EqualOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.equal", odsAttrs.getContext());
  }

  EqualOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class EqualOpGenericAdaptor : public detail::EqualOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::EqualOpGenericAdaptorBase;
public:
  EqualOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  EqualOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : EqualOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = EqualOp, typename = std::enable_if_t<std::is_same_v<LateInst, EqualOp>>>
  EqualOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getLhs() {
    return (*getODSOperands(0).begin());
  }

  ValueT getRhs() {
    return (*getODSOperands(1).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class EqualOpAdaptor : public EqualOpGenericAdaptor<::mlir::ValueRange> {
public:
  using EqualOpGenericAdaptor::EqualOpGenericAdaptor;
  EqualOpAdaptor(EqualOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class EqualOp : public ::mlir::Op<EqualOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::NOperands<2>::Impl, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::OpTrait::SameTypeOperands> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = EqualOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = EqualOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.equal");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getLhs() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(0).begin());
  }

  ::mlir::Value getRhs() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(1).begin());
  }

  ::mlir::OpOperand &getLhsMutable() {
    auto range = getODSOperandIndexAndLength(0);
    return getOperation()->getOpOperand(range.first);
  }

  ::mlir::OpOperand &getRhsMutable() {
    auto range = getODSOperandIndexAndLength(1);
    return getOperation()->getOpOperand(range.first);
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getResult() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(0).begin());
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type result, ::mlir::Value lhs, ::mlir::Value rhs);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value lhs, ::mlir::Value rhs);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  ::mlir::OpFoldResult fold(FoldAdaptor adaptor);
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &_odsPrinter);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::EqualOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::GetElementOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class GetElementOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  GetElementOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.get_element", odsAttrs.getContext());
  }

  GetElementOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class GetElementOpGenericAdaptor : public detail::GetElementOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::GetElementOpGenericAdaptorBase;
public:
  GetElementOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  GetElementOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : GetElementOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = GetElementOp, typename = std::enable_if_t<std::is_same_v<LateInst, GetElementOp>>>
  GetElementOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getTensorList() {
    return (*getODSOperands(0).begin());
  }

  ValueT getIndex() {
    return (*getODSOperands(1).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class GetElementOpAdaptor : public GetElementOpGenericAdaptor<::mlir::ValueRange> {
public:
  using GetElementOpGenericAdaptor::GetElementOpGenericAdaptor;
  GetElementOpAdaptor(GetElementOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class GetElementOp : public ::mlir::Op<GetElementOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::NOperands<2>::Impl, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::InferTypeOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = GetElementOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = GetElementOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.get_element");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getTensorList() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(0).begin());
  }

  ::mlir::TypedValue<::mlir::IndexType> getIndex() {
    return ::llvm::cast<::mlir::TypedValue<::mlir::IndexType>>(*getODSOperands(1).begin());
  }

  ::mlir::OpOperand &getTensorListMutable() {
    auto range = getODSOperandIndexAndLength(0);
    return getOperation()->getOpOperand(range.first);
  }

  ::mlir::OpOperand &getIndexMutable() {
    auto range = getODSOperandIndexAndLength(1);
    return getOperation()->getOpOperand(range.first);
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getOut() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(0).begin());
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type out, ::mlir::Value tensor_list, ::mlir::Value index);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value tensor_list, ::mlir::Value index);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value tensor_list, ::mlir::Value index);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static void getCanonicalizationPatterns(::mlir::RewritePatternSet &results, ::mlir::MLIRContext *context);
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &_odsPrinter);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::GetElementOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::GetElementTypeOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class GetElementTypeOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  GetElementTypeOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.get_element_type", odsAttrs.getContext());
  }

  GetElementTypeOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class GetElementTypeOpGenericAdaptor : public detail::GetElementTypeOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::GetElementTypeOpGenericAdaptorBase;
public:
  GetElementTypeOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  GetElementTypeOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : GetElementTypeOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = GetElementTypeOp, typename = std::enable_if_t<std::is_same_v<LateInst, GetElementTypeOp>>>
  GetElementTypeOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getArg() {
    return (*getODSOperands(0).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class GetElementTypeOpAdaptor : public GetElementTypeOpGenericAdaptor<::mlir::ValueRange> {
public:
  using GetElementTypeOpGenericAdaptor::GetElementTypeOpGenericAdaptor;
  GetElementTypeOpAdaptor(GetElementTypeOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class GetElementTypeOp : public ::mlir::Op<GetElementTypeOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::InferTypeOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = GetElementTypeOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = GetElementTypeOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.get_element_type");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getArg() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(0).begin());
  }

  ::mlir::OpOperand &getArgMutable() {
    auto range = getODSOperandIndexAndLength(0);
    return getOperation()->getOpOperand(range.first);
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getOut() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(0).begin());
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type out, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &_odsPrinter);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::GetElementTypeOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::GetLengthOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class GetLengthOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  GetLengthOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.get_length", odsAttrs.getContext());
  }

  GetLengthOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class GetLengthOpGenericAdaptor : public detail::GetLengthOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::GetLengthOpGenericAdaptorBase;
public:
  GetLengthOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  GetLengthOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : GetLengthOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = GetLengthOp, typename = std::enable_if_t<std::is_same_v<LateInst, GetLengthOp>>>
  GetLengthOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getTensorList() {
    return (*getODSOperands(0).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class GetLengthOpAdaptor : public GetLengthOpGenericAdaptor<::mlir::ValueRange> {
public:
  using GetLengthOpGenericAdaptor::GetLengthOpGenericAdaptor;
  GetLengthOpAdaptor(GetLengthOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class GetLengthOp : public ::mlir::Op<GetLengthOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::IndexType>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::InferTypeOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = GetLengthOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = GetLengthOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.get_length");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getTensorList() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(0).begin());
  }

  ::mlir::OpOperand &getTensorListMutable() {
    auto range = getODSOperandIndexAndLength(0);
    return getOperation()->getOpOperand(range.first);
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::TypedValue<::mlir::IndexType> getOut() {
    return ::llvm::cast<::mlir::TypedValue<::mlir::IndexType>>(*getODSResults(0).begin());
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type out, ::mlir::Value tensor_list);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value tensor_list);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value tensor_list);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static void getCanonicalizationPatterns(::mlir::RewritePatternSet &results, ::mlir::MLIRContext *context);
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &_odsPrinter);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::GetLengthOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::GetShapeOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class GetShapeOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  GetShapeOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.get_shape", odsAttrs.getContext());
  }

  GetShapeOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class GetShapeOpGenericAdaptor : public detail::GetShapeOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::GetShapeOpGenericAdaptorBase;
public:
  GetShapeOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  GetShapeOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : GetShapeOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = GetShapeOp, typename = std::enable_if_t<std::is_same_v<LateInst, GetShapeOp>>>
  GetShapeOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getArg() {
    return (*getODSOperands(0).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class GetShapeOpAdaptor : public GetShapeOpGenericAdaptor<::mlir::ValueRange> {
public:
  using GetShapeOpGenericAdaptor::GetShapeOpGenericAdaptor;
  GetShapeOpAdaptor(GetShapeOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class GetShapeOp : public ::mlir::Op<GetShapeOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::shape::ShapeType>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::InferTypeOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = GetShapeOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = GetShapeOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.get_shape");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getArg() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(0).begin());
  }

  ::mlir::OpOperand &getArgMutable() {
    auto range = getODSOperandIndexAndLength(0);
    return getOperation()->getOpOperand(range.first);
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::TypedValue<::mlir::shape::ShapeType> getOut() {
    return ::llvm::cast<::mlir::TypedValue<::mlir::shape::ShapeType>>(*getODSResults(0).begin());
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type out, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static void getCanonicalizationPatterns(::mlir::RewritePatternSet &results, ::mlir::MLIRContext *context);
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &_odsPrinter);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::GetShapeOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::TFRFuncOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class TFRFuncOpGenericAdaptorBase {
public:
  struct Properties {
    using arg_attrsTy = ::mlir::ArrayAttr;
    arg_attrsTy arg_attrs;

    auto getArgAttrs() {
      auto &propStorage = this->arg_attrs;
      return ::llvm::dyn_cast_or_null<::mlir::ArrayAttr>(propStorage);
    }
    void setArgAttrs(const ::mlir::ArrayAttr &propValue) {
      this->arg_attrs = propValue;
    }
    using function_typeTy = ::mlir::TypeAttr;
    function_typeTy function_type;

    auto getFunctionType() {
      auto &propStorage = this->function_type;
      return ::llvm::cast<::mlir::TypeAttr>(propStorage);
    }
    void setFunctionType(const ::mlir::TypeAttr &propValue) {
      this->function_type = propValue;
    }
    using res_attrsTy = ::mlir::ArrayAttr;
    res_attrsTy res_attrs;

    auto getResAttrs() {
      auto &propStorage = this->res_attrs;
      return ::llvm::dyn_cast_or_null<::mlir::ArrayAttr>(propStorage);
    }
    void setResAttrs(const ::mlir::ArrayAttr &propValue) {
      this->res_attrs = propValue;
    }
    using sym_nameTy = ::mlir::StringAttr;
    sym_nameTy sym_name;

    auto getSymName() {
      auto &propStorage = this->sym_name;
      return ::llvm::cast<::mlir::StringAttr>(propStorage);
    }
    void setSymName(const ::mlir::StringAttr &propValue) {
      this->sym_name = propValue;
    }
    bool operator==(const Properties &rhs) const {
      return 
        rhs.arg_attrs == this->arg_attrs &&
        rhs.function_type == this->function_type &&
        rhs.res_attrs == this->res_attrs &&
        rhs.sym_name == this->sym_name &&
        true;
    }
    bool operator!=(const Properties &rhs) const {
      return !(*this == rhs);
    }
  };
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  Properties properties;
  ::mlir::RegionRange odsRegions;
public:
  TFRFuncOpGenericAdaptorBase(::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), properties(properties), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.func", odsAttrs.getContext());
  }

  TFRFuncOpGenericAdaptorBase(TFRFuncOp op);

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  const Properties &getProperties() {
    return properties;
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

  ::mlir::TypeAttr getFunctionTypeAttr() {
    auto attr = ::llvm::cast<::mlir::TypeAttr>(getProperties().function_type);
    return attr;
  }

  ::mlir::FunctionType getFunctionType();
  ::mlir::StringAttr getSymNameAttr() {
    auto attr = ::llvm::cast<::mlir::StringAttr>(getProperties().sym_name);
    return attr;
  }

  ::llvm::StringRef getSymName();
  ::mlir::ArrayAttr getArgAttrsAttr() {
    auto attr = ::llvm::dyn_cast_or_null<::mlir::ArrayAttr>(getProperties().arg_attrs);
    return attr;
  }

  ::std::optional< ::mlir::ArrayAttr > getArgAttrs();
  ::mlir::ArrayAttr getResAttrsAttr() {
    auto attr = ::llvm::dyn_cast_or_null<::mlir::ArrayAttr>(getProperties().res_attrs);
    return attr;
  }

  ::std::optional< ::mlir::ArrayAttr > getResAttrs();
  ::mlir::RegionRange getBody() {
    return odsRegions.drop_front(0);
  }

  ::mlir::RegionRange getRegions() {
    return odsRegions;
  }

};
} // namespace detail
template <typename RangeT>
class TFRFuncOpGenericAdaptor : public detail::TFRFuncOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::TFRFuncOpGenericAdaptorBase;
public:
  TFRFuncOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  TFRFuncOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : TFRFuncOpGenericAdaptor(values, attrs, (properties ? *properties.as<Properties *>() : Properties{}), regions) {}

  TFRFuncOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = nullptr) : TFRFuncOpGenericAdaptor(values, attrs, Properties{}, {}) {}

  template <typename LateInst = TFRFuncOp, typename = std::enable_if_t<std::is_same_v<LateInst, TFRFuncOp>>>
  TFRFuncOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class TFRFuncOpAdaptor : public TFRFuncOpGenericAdaptor<::mlir::ValueRange> {
public:
  using TFRFuncOpGenericAdaptor::TFRFuncOpGenericAdaptor;
  TFRFuncOpAdaptor(TFRFuncOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class TFRFuncOp : public ::mlir::Op<TFRFuncOp, ::mlir::OpTrait::VariadicRegions, ::mlir::OpTrait::ZeroResults, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::ZeroOperands, ::mlir::OpTrait::HasParent<ModuleOp>::Impl, ::mlir::OpTrait::OpInvariants, ::mlir::BytecodeOpInterface::Trait, ::mlir::SymbolOpInterface::Trait, ::mlir::CallableOpInterface::Trait, ::mlir::FunctionOpInterface::Trait, ::mlir::OpTrait::IsIsolatedFromAbove> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = TFRFuncOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = TFRFuncOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  using Properties = FoldAdaptor::Properties;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    static ::llvm::StringRef attrNames[] = {::llvm::StringRef("arg_attrs"), ::llvm::StringRef("function_type"), ::llvm::StringRef("res_attrs"), ::llvm::StringRef("sym_name")};
    return ::llvm::ArrayRef(attrNames);
  }

  ::mlir::StringAttr getArgAttrsAttrName() {
    return getAttributeNameForIndex(0);
  }

  static ::mlir::StringAttr getArgAttrsAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 0);
  }

  ::mlir::StringAttr getFunctionTypeAttrName() {
    return getAttributeNameForIndex(1);
  }

  static ::mlir::StringAttr getFunctionTypeAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 1);
  }

  ::mlir::StringAttr getResAttrsAttrName() {
    return getAttributeNameForIndex(2);
  }

  static ::mlir::StringAttr getResAttrsAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 2);
  }

  ::mlir::StringAttr getSymNameAttrName() {
    return getAttributeNameForIndex(3);
  }

  static ::mlir::StringAttr getSymNameAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 3);
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.func");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::MutableArrayRef<::mlir::Region> getBody() {
    return (*this)->getRegions().drop_front(0);
  }

  static ::mlir::LogicalResult setPropertiesFromAttr(Properties &prop, ::mlir::Attribute attr, ::llvm::function_ref<::mlir::InFlightDiagnostic()> emitError);
  static ::mlir::Attribute getPropertiesAsAttr(::mlir::MLIRContext *ctx, const Properties &prop);
  static llvm::hash_code computePropertiesHash(const Properties &prop);
  static std::optional<mlir::Attribute> getInherentAttr(::mlir::MLIRContext *ctx, const Properties &prop, llvm::StringRef name);
  static void setInherentAttr(Properties &prop, llvm::StringRef name, mlir::Attribute value);
  static void populateInherentAttrs(::mlir::MLIRContext *ctx, const Properties &prop, ::mlir::NamedAttrList &attrs);
  static ::mlir::LogicalResult verifyInherentAttrs(::mlir::OperationName opName, ::mlir::NamedAttrList &attrs, llvm::function_ref<::mlir::InFlightDiagnostic()> emitError);
  static ::mlir::LogicalResult readProperties(::mlir::DialectBytecodeReader &reader, ::mlir::OperationState &state);
  void writeProperties(::mlir::DialectBytecodeWriter &writer);
  ::mlir::TypeAttr getFunctionTypeAttr() {
    return ::llvm::cast<::mlir::TypeAttr>(getProperties().function_type);
  }

  ::mlir::FunctionType getFunctionType();
  ::mlir::StringAttr getSymNameAttr() {
    return ::llvm::cast<::mlir::StringAttr>(getProperties().sym_name);
  }

  ::llvm::StringRef getSymName();
  ::mlir::ArrayAttr getArgAttrsAttr() {
    return ::llvm::dyn_cast_or_null<::mlir::ArrayAttr>(getProperties().arg_attrs);
  }

  ::std::optional< ::mlir::ArrayAttr > getArgAttrs();
  ::mlir::ArrayAttr getResAttrsAttr() {
    return ::llvm::dyn_cast_or_null<::mlir::ArrayAttr>(getProperties().res_attrs);
  }

  ::std::optional< ::mlir::ArrayAttr > getResAttrs();
  void setFunctionTypeAttr(::mlir::TypeAttr attr) {
    getProperties().function_type = attr;
  }

  void setFunctionType(::mlir::FunctionType attrValue);
  void setSymNameAttr(::mlir::StringAttr attr) {
    getProperties().sym_name = attr;
  }

  void setSymName(::llvm::StringRef attrValue);
  void setArgAttrsAttr(::mlir::ArrayAttr attr) {
    getProperties().arg_attrs = attr;
  }

  void setResAttrsAttr(::mlir::ArrayAttr attr) {
    getProperties().res_attrs = attr;
  }

  ::mlir::Attribute removeArgAttrsAttr() {
      auto &attr = getProperties().arg_attrs;
      attr = {};
      return attr;
  }

  ::mlir::Attribute removeResAttrsAttr() {
      auto &attr = getProperties().res_attrs;
      attr = {};
      return attr;
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, StringRef name, FunctionType type, ArrayRef<NamedAttribute> attrs = {});
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &p);
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  ::mlir::LogicalResult verify();
private:
  ::mlir::StringAttr getAttributeNameForIndex(unsigned index) {
    return getAttributeNameForIndex((*this)->getName(), index);
  }

  static ::mlir::StringAttr getAttributeNameForIndex(::mlir::OperationName name, unsigned index) {
    assert(index < 4 && "invalid attribute index");
    assert(name.getStringRef() == getOperationName() && "invalid operation name");
    assert(name.isRegistered() && "Operation isn't registered, missing a "
          "dependent dialect loading?");
    return name.getAttributeNames()[index];
  }

public:
  LogicalResult verifyType() { return success(); }

  mlir::Region *getCallableRegion();

  /// Returns the argument types of this function.
  ArrayRef<Type> getArgumentTypes() { return getFunctionType().getInputs(); }

  /// Returns the result types of this function.
  ArrayRef<Type> getResultTypes() { return getFunctionType().getResults(); }

  // Get the names of all defined attributes, including both derived and
  // non-derived ones.
  llvm::StringSet<> getDefinedAttributeNames() {
    llvm::StringSet<> all_attrs;
    for (auto& attr : (*this)->getAttrs()) {
      all_attrs.insert(attr.getName().strref());
    }
    for (const auto& operand : llvm::enumerate(getArgumentTypes())) {
      if (auto attr_name = getArgAttrOfType<StringAttr>(
          operand.index(), kAttrArgumentNameAttr)) {
        all_attrs.insert(attr_name.getValue());
      }
    }
    return all_attrs;
  }
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::TFRFuncOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::TFRQuantActRangeOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class TFRQuantActRangeOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  TFRQuantActRangeOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.quant_act_range", odsAttrs.getContext());
  }

  TFRQuantActRangeOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class TFRQuantActRangeOpGenericAdaptor : public detail::TFRQuantActRangeOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::TFRQuantActRangeOpGenericAdaptorBase;
public:
  TFRQuantActRangeOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  TFRQuantActRangeOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : TFRQuantActRangeOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = TFRQuantActRangeOp, typename = std::enable_if_t<std::is_same_v<LateInst, TFRQuantActRangeOp>>>
  TFRQuantActRangeOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getAct() {
    return (*getODSOperands(0).begin());
  }

  ValueT getScale() {
    return (*getODSOperands(1).begin());
  }

  ValueT getZp() {
    return (*getODSOperands(2).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class TFRQuantActRangeOpAdaptor : public TFRQuantActRangeOpGenericAdaptor<::mlir::ValueRange> {
public:
  using TFRQuantActRangeOpGenericAdaptor::TFRQuantActRangeOpGenericAdaptor;
  TFRQuantActRangeOpAdaptor(TFRQuantActRangeOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class TFRQuantActRangeOp : public ::mlir::Op<TFRQuantActRangeOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::NResults<2>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::NOperands<3>::Impl, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::InferTypeOpInterface::Trait, ::mlir::OpAsmOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = TFRQuantActRangeOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = TFRQuantActRangeOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  void getAsmResultNames(::mlir::OpAsmSetValueNameFn setNameFn);
  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.quant_act_range");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getAct() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(0).begin());
  }

  ::mlir::TypedValue<::mlir::FloatType> getScale() {
    return ::llvm::cast<::mlir::TypedValue<::mlir::FloatType>>(*getODSOperands(1).begin());
  }

  ::mlir::TypedValue<::mlir::IntegerType> getZp() {
    return ::llvm::cast<::mlir::TypedValue<::mlir::IntegerType>>(*getODSOperands(2).begin());
  }

  ::mlir::OpOperand &getActMutable() {
    auto range = getODSOperandIndexAndLength(0);
    return getOperation()->getOpOperand(range.first);
  }

  ::mlir::OpOperand &getScaleMutable() {
    auto range = getODSOperandIndexAndLength(1);
    return getOperation()->getOpOperand(range.first);
  }

  ::mlir::OpOperand &getZpMutable() {
    auto range = getODSOperandIndexAndLength(2);
    return getOperation()->getOpOperand(range.first);
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getMin() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(0).begin());
  }

  ::mlir::Value getMax() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(1).begin());
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type min, ::mlir::Type max, ::mlir::Value act, ::mlir::Value scale, ::mlir::Value zp);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value act, ::mlir::Value scale, ::mlir::Value zp);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value act, ::mlir::Value scale, ::mlir::Value zp);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &_odsPrinter);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::TFRQuantActRangeOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::TFRQuantQParamsOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class TFRQuantQParamsOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  TFRQuantQParamsOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.quant_qparam", odsAttrs.getContext());
  }

  TFRQuantQParamsOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class TFRQuantQParamsOpGenericAdaptor : public detail::TFRQuantQParamsOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::TFRQuantQParamsOpGenericAdaptorBase;
public:
  TFRQuantQParamsOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  TFRQuantQParamsOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : TFRQuantQParamsOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = TFRQuantQParamsOp, typename = std::enable_if_t<std::is_same_v<LateInst, TFRQuantQParamsOp>>>
  TFRQuantQParamsOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getInput() {
    return (*getODSOperands(0).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class TFRQuantQParamsOpAdaptor : public TFRQuantQParamsOpGenericAdaptor<::mlir::ValueRange> {
public:
  using TFRQuantQParamsOpGenericAdaptor::TFRQuantQParamsOpGenericAdaptor;
  TFRQuantQParamsOpAdaptor(TFRQuantQParamsOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class TFRQuantQParamsOp : public ::mlir::Op<TFRQuantQParamsOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::NResults<2>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::InferTypeOpInterface::Trait, ::mlir::OpAsmOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = TFRQuantQParamsOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = TFRQuantQParamsOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  void getAsmResultNames(::mlir::OpAsmSetValueNameFn setNameFn);
  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.quant_qparam");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getInput() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(0).begin());
  }

  ::mlir::OpOperand &getInputMutable() {
    auto range = getODSOperandIndexAndLength(0);
    return getOperation()->getOpOperand(range.first);
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getScale() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(0).begin());
  }

  ::mlir::Value getZp() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(1).begin());
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type scale, ::mlir::Type zp, ::mlir::Value input);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value input);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value input);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static void getCanonicalizationPatterns(::mlir::RewritePatternSet &results, ::mlir::MLIRContext *context);
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &_odsPrinter);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::TFRQuantQParamsOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::TFRQuantRawDataOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class TFRQuantRawDataOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  TFRQuantRawDataOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.quant_raw_data", odsAttrs.getContext());
  }

  TFRQuantRawDataOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class TFRQuantRawDataOpGenericAdaptor : public detail::TFRQuantRawDataOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::TFRQuantRawDataOpGenericAdaptorBase;
public:
  TFRQuantRawDataOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  TFRQuantRawDataOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : TFRQuantRawDataOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = TFRQuantRawDataOp, typename = std::enable_if_t<std::is_same_v<LateInst, TFRQuantRawDataOp>>>
  TFRQuantRawDataOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getInput() {
    return (*getODSOperands(0).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class TFRQuantRawDataOpAdaptor : public TFRQuantRawDataOpGenericAdaptor<::mlir::ValueRange> {
public:
  using TFRQuantRawDataOpGenericAdaptor::TFRQuantRawDataOpGenericAdaptor;
  TFRQuantRawDataOpAdaptor(TFRQuantRawDataOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class TFRQuantRawDataOp : public ::mlir::Op<TFRQuantRawDataOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::OpTrait::SameOperandsAndResultType, ::mlir::InferTypeOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = TFRQuantRawDataOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = TFRQuantRawDataOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.quant_raw_data");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getInput() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(0).begin());
  }

  ::mlir::OpOperand &getInputMutable() {
    auto range = getODSOperandIndexAndLength(0);
    return getOperation()->getOpOperand(range.first);
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getOutput() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(0).begin());
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type output, ::mlir::Value input);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value input);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value input);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static void getCanonicalizationPatterns(::mlir::RewritePatternSet &results, ::mlir::MLIRContext *context);
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &_odsPrinter);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::TFRQuantRawDataOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::TFRQuantRescaleOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class TFRQuantRescaleOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  TFRQuantRescaleOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.quant_rescale", odsAttrs.getContext());
  }

  TFRQuantRescaleOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class TFRQuantRescaleOpGenericAdaptor : public detail::TFRQuantRescaleOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::TFRQuantRescaleOpGenericAdaptorBase;
public:
  TFRQuantRescaleOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  TFRQuantRescaleOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : TFRQuantRescaleOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = TFRQuantRescaleOp, typename = std::enable_if_t<std::is_same_v<LateInst, TFRQuantRescaleOp>>>
  TFRQuantRescaleOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getInput() {
    return (*getODSOperands(0).begin());
  }

  ValueT getScale() {
    return (*getODSOperands(1).begin());
  }

  ValueT getZp() {
    return (*getODSOperands(2).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class TFRQuantRescaleOpAdaptor : public TFRQuantRescaleOpGenericAdaptor<::mlir::ValueRange> {
public:
  using TFRQuantRescaleOpGenericAdaptor::TFRQuantRescaleOpGenericAdaptor;
  TFRQuantRescaleOpAdaptor(TFRQuantRescaleOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class TFRQuantRescaleOp : public ::mlir::Op<TFRQuantRescaleOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::NOperands<3>::Impl, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::InferTypeOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = TFRQuantRescaleOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = TFRQuantRescaleOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.quant_rescale");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getInput() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(0).begin());
  }

  ::mlir::Value getScale() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(1).begin());
  }

  ::mlir::TypedValue<::mlir::IntegerType> getZp() {
    return ::llvm::cast<::mlir::TypedValue<::mlir::IntegerType>>(*getODSOperands(2).begin());
  }

  ::mlir::OpOperand &getInputMutable() {
    auto range = getODSOperandIndexAndLength(0);
    return getOperation()->getOpOperand(range.first);
  }

  ::mlir::OpOperand &getScaleMutable() {
    auto range = getODSOperandIndexAndLength(1);
    return getOperation()->getOpOperand(range.first);
  }

  ::mlir::OpOperand &getZpMutable() {
    auto range = getODSOperandIndexAndLength(2);
    return getOperation()->getOpOperand(range.first);
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getOutput() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(0).begin());
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type output, ::mlir::Value input, ::mlir::Value scale, ::mlir::Value zp);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value input, ::mlir::Value scale, ::mlir::Value zp);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value input, ::mlir::Value scale, ::mlir::Value zp);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static void getCanonicalizationPatterns(::mlir::RewritePatternSet &results, ::mlir::MLIRContext *context);
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &_odsPrinter);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::TFRQuantRescaleOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::TFRQuantScaleFactorOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class TFRQuantScaleFactorOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  TFRQuantScaleFactorOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.quant_scale_factor", odsAttrs.getContext());
  }

  TFRQuantScaleFactorOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class TFRQuantScaleFactorOpGenericAdaptor : public detail::TFRQuantScaleFactorOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::TFRQuantScaleFactorOpGenericAdaptorBase;
public:
  TFRQuantScaleFactorOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  TFRQuantScaleFactorOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : TFRQuantScaleFactorOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = TFRQuantScaleFactorOp, typename = std::enable_if_t<std::is_same_v<LateInst, TFRQuantScaleFactorOp>>>
  TFRQuantScaleFactorOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getOutScale() {
    return (*getODSOperands(0).begin());
  }

  ValueT getInScales() {
    return (*getODSOperands(1).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class TFRQuantScaleFactorOpAdaptor : public TFRQuantScaleFactorOpGenericAdaptor<::mlir::ValueRange> {
public:
  using TFRQuantScaleFactorOpGenericAdaptor::TFRQuantScaleFactorOpGenericAdaptor;
  TFRQuantScaleFactorOpAdaptor(TFRQuantScaleFactorOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class TFRQuantScaleFactorOp : public ::mlir::Op<TFRQuantScaleFactorOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::NOperands<2>::Impl, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::InferTypeOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = TFRQuantScaleFactorOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = TFRQuantScaleFactorOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.quant_scale_factor");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::TypedValue<::mlir::FloatType> getOutScale() {
    return ::llvm::cast<::mlir::TypedValue<::mlir::FloatType>>(*getODSOperands(0).begin());
  }

  ::mlir::Value getInScales() {
    return ::llvm::cast<::mlir::Value>(*getODSOperands(1).begin());
  }

  ::mlir::OpOperand &getOutScaleMutable() {
    auto range = getODSOperandIndexAndLength(0);
    return getOperation()->getOpOperand(range.first);
  }

  ::mlir::OpOperand &getInScalesMutable() {
    auto range = getODSOperandIndexAndLength(1);
    return getOperation()->getOpOperand(range.first);
  }

  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Value getScaleFactor() {
    return ::llvm::cast<::mlir::Value>(*getODSResults(0).begin());
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type scale_factor, ::mlir::Value out_scale, ::mlir::Value in_scales);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value out_scale, ::mlir::Value in_scales);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value out_scale, ::mlir::Value in_scales);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static void getCanonicalizationPatterns(::mlir::RewritePatternSet &results, ::mlir::MLIRContext *context);
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &_odsPrinter);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::TFRQuantScaleFactorOp)

namespace mlir {
namespace TFR {

//===----------------------------------------------------------------------===//
// ::mlir::TFR::TFRReturnOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class TFRReturnOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  TFRReturnOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("tfr.return", odsAttrs.getContext());
  }

  TFRReturnOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize);
  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class TFRReturnOpGenericAdaptor : public detail::TFRReturnOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::TFRReturnOpGenericAdaptorBase;
public:
  TFRReturnOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  TFRReturnOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : TFRReturnOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = TFRReturnOp, typename = std::enable_if_t<std::is_same_v<LateInst, TFRReturnOp>>>
  TFRReturnOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  RangeT getArguments() {
    return getODSOperands(0);
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class TFRReturnOpAdaptor : public TFRReturnOpGenericAdaptor<::mlir::ValueRange> {
public:
  using TFRReturnOpGenericAdaptor::TFRReturnOpGenericAdaptor;
  TFRReturnOpAdaptor(TFRReturnOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class TFRReturnOp : public ::mlir::Op<TFRReturnOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::ZeroResults, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::VariadicOperands, ::mlir::OpTrait::HasParent<TFRFuncOp>::Impl, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::RegionBranchTerminatorOpInterface::Trait, ::mlir::OpTrait::ReturnLike, ::mlir::OpTrait::IsTerminator> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = TFRReturnOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = TFRReturnOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("tfr.return");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index);
  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Operation::operand_range getArguments() {
    return getODSOperands(0);
  }

  ::mlir::MutableOperandRange getArgumentsMutable();
  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange arguments);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  ::mlir::MutableOperandRange getMutableSuccessorOperands(::mlir::RegionBranchPoint point);
  static ::mlir::ParseResult parse(::mlir::OpAsmParser &parser, ::mlir::OperationState &result);
  void print(::mlir::OpAsmPrinter &_odsPrinter);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace TFR
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::TFR::TFRReturnOp)


#endif  // GET_OP_CLASSES

