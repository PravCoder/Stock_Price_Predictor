/*===- TableGen'erated file -------------------------------------*- C++ -*-===*\
|*                                                                            *|
|* Op Declarations                                                            *|
|*                                                                            *|
|* Automatically generated file, do not edit!                                 *|
|* From: QuantOps.td                                                          *|
|*                                                                            *|
\*===----------------------------------------------------------------------===*/

namespace mlir {
namespace quantfork {
class ConstFakeQuant;
} // namespace quantfork
} // namespace mlir
namespace mlir {
namespace quantfork {
class ConstFakeQuantPerAxis;
} // namespace quantfork
} // namespace mlir
namespace mlir {
namespace quantfork {
class CoupledRefOp;
} // namespace quantfork
} // namespace mlir
namespace mlir {
namespace quantfork {
class DequantizeCastOp;
} // namespace quantfork
} // namespace mlir
namespace mlir {
namespace quantfork {
class QuantizeCastOp;
} // namespace quantfork
} // namespace mlir
namespace mlir {
namespace quantfork {
class QuantizeRegionOp;
} // namespace quantfork
} // namespace mlir
namespace mlir {
namespace quantfork {
class ReturnOp;
} // namespace quantfork
} // namespace mlir
namespace mlir {
namespace quantfork {
class StatisticsOp;
} // namespace quantfork
} // namespace mlir
namespace mlir {
namespace quantfork {
class StatisticsRefOp;
} // namespace quantfork
} // namespace mlir
namespace mlir {
namespace quantfork {
class StorageCastOp;
} // namespace quantfork
} // namespace mlir
#ifdef GET_OP_CLASSES
#undef GET_OP_CLASSES

namespace mlir {
namespace quantfork {

//===----------------------------------------------------------------------===//
// ::mlir::quantfork::ConstFakeQuant declarations
//===----------------------------------------------------------------------===//

namespace detail {
class ConstFakeQuantGenericAdaptorBase {
public:
  struct Properties {
    using is_signedTy = ::mlir::BoolAttr;
    is_signedTy is_signed;

    auto getIsSigned() {
      auto &propStorage = this->is_signed;
      return ::llvm::dyn_cast_or_null<::mlir::BoolAttr>(propStorage);
    }
    void setIsSigned(const ::mlir::BoolAttr &propValue) {
      this->is_signed = propValue;
    }
    using maxTy = ::mlir::FloatAttr;
    maxTy max;

    auto getMax() {
      auto &propStorage = this->max;
      return ::llvm::cast<::mlir::FloatAttr>(propStorage);
    }
    void setMax(const ::mlir::FloatAttr &propValue) {
      this->max = propValue;
    }
    using minTy = ::mlir::FloatAttr;
    minTy min;

    auto getMin() {
      auto &propStorage = this->min;
      return ::llvm::cast<::mlir::FloatAttr>(propStorage);
    }
    void setMin(const ::mlir::FloatAttr &propValue) {
      this->min = propValue;
    }
    using narrow_rangeTy = ::mlir::BoolAttr;
    narrow_rangeTy narrow_range;

    auto getNarrowRange() {
      auto &propStorage = this->narrow_range;
      return ::llvm::dyn_cast_or_null<::mlir::BoolAttr>(propStorage);
    }
    void setNarrowRange(const ::mlir::BoolAttr &propValue) {
      this->narrow_range = propValue;
    }
    using num_bitsTy = ::mlir::IntegerAttr;
    num_bitsTy num_bits;

    auto getNumBits() {
      auto &propStorage = this->num_bits;
      return ::llvm::cast<::mlir::IntegerAttr>(propStorage);
    }
    void setNumBits(const ::mlir::IntegerAttr &propValue) {
      this->num_bits = propValue;
    }
    bool operator==(const Properties &rhs) const {
      return 
        rhs.is_signed == this->is_signed &&
        rhs.max == this->max &&
        rhs.min == this->min &&
        rhs.narrow_range == this->narrow_range &&
        rhs.num_bits == this->num_bits &&
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
  ConstFakeQuantGenericAdaptorBase(::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), properties(properties), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("quantfork.const_fake_quant", odsAttrs.getContext());
  }

  ConstFakeQuantGenericAdaptorBase(ConstFakeQuant op);

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  const Properties &getProperties() {
    return properties;
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

  ::mlir::FloatAttr getMinAttr() {
    auto attr = ::llvm::cast<::mlir::FloatAttr>(getProperties().min);
    return attr;
  }

  ::llvm::APFloat getMin();
  ::mlir::FloatAttr getMaxAttr() {
    auto attr = ::llvm::cast<::mlir::FloatAttr>(getProperties().max);
    return attr;
  }

  ::llvm::APFloat getMax();
  ::mlir::IntegerAttr getNumBitsAttr() {
    auto attr = ::llvm::cast<::mlir::IntegerAttr>(getProperties().num_bits);
    return attr;
  }

  uint64_t getNumBits();
  ::mlir::BoolAttr getNarrowRangeAttr();
  bool getNarrowRange();
  ::mlir::BoolAttr getIsSignedAttr();
  bool getIsSigned();
};
} // namespace detail
template <typename RangeT>
class ConstFakeQuantGenericAdaptor : public detail::ConstFakeQuantGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::ConstFakeQuantGenericAdaptorBase;
public:
  ConstFakeQuantGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  ConstFakeQuantGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : ConstFakeQuantGenericAdaptor(values, attrs, (properties ? *properties.as<Properties *>() : Properties{}), regions) {}

  ConstFakeQuantGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = nullptr) : ConstFakeQuantGenericAdaptor(values, attrs, Properties{}, {}) {}

  template <typename LateInst = ConstFakeQuant, typename = std::enable_if_t<std::is_same_v<LateInst, ConstFakeQuant>>>
  ConstFakeQuantGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getInputs() {
    return (*getODSOperands(0).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class ConstFakeQuantAdaptor : public ConstFakeQuantGenericAdaptor<::mlir::ValueRange> {
public:
  using ConstFakeQuantGenericAdaptor::ConstFakeQuantGenericAdaptor;
  ConstFakeQuantAdaptor(ConstFakeQuant op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class ConstFakeQuant : public ::mlir::Op<ConstFakeQuant, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::TensorType>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::BytecodeOpInterface::Trait, ::mlir::OpTrait::SameOperandsAndResultType, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::InferTypeOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = ConstFakeQuantAdaptor;
  template <typename RangeT>
  using GenericAdaptor = ConstFakeQuantGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  using Properties = FoldAdaptor::Properties;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    static ::llvm::StringRef attrNames[] = {::llvm::StringRef("is_signed"), ::llvm::StringRef("max"), ::llvm::StringRef("min"), ::llvm::StringRef("narrow_range"), ::llvm::StringRef("num_bits")};
    return ::llvm::ArrayRef(attrNames);
  }

  ::mlir::StringAttr getIsSignedAttrName() {
    return getAttributeNameForIndex(0);
  }

  static ::mlir::StringAttr getIsSignedAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 0);
  }

  ::mlir::StringAttr getMaxAttrName() {
    return getAttributeNameForIndex(1);
  }

  static ::mlir::StringAttr getMaxAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 1);
  }

  ::mlir::StringAttr getMinAttrName() {
    return getAttributeNameForIndex(2);
  }

  static ::mlir::StringAttr getMinAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 2);
  }

  ::mlir::StringAttr getNarrowRangeAttrName() {
    return getAttributeNameForIndex(3);
  }

  static ::mlir::StringAttr getNarrowRangeAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 3);
  }

  ::mlir::StringAttr getNumBitsAttrName() {
    return getAttributeNameForIndex(4);
  }

  static ::mlir::StringAttr getNumBitsAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 4);
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("quantfork.const_fake_quant");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::TypedValue<::mlir::TensorType> getInputs() {
    return ::llvm::cast<::mlir::TypedValue<::mlir::TensorType>>(*getODSOperands(0).begin());
  }

  ::mlir::OpOperand &getInputsMutable() {
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

  ::mlir::TypedValue<::mlir::TensorType> getOutputs() {
    return ::llvm::cast<::mlir::TypedValue<::mlir::TensorType>>(*getODSResults(0).begin());
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
  ::mlir::FloatAttr getMinAttr() {
    return ::llvm::cast<::mlir::FloatAttr>(getProperties().min);
  }

  ::llvm::APFloat getMin();
  ::mlir::FloatAttr getMaxAttr() {
    return ::llvm::cast<::mlir::FloatAttr>(getProperties().max);
  }

  ::llvm::APFloat getMax();
  ::mlir::IntegerAttr getNumBitsAttr() {
    return ::llvm::cast<::mlir::IntegerAttr>(getProperties().num_bits);
  }

  uint64_t getNumBits();
  ::mlir::BoolAttr getNarrowRangeAttr() {
    return ::llvm::dyn_cast_or_null<::mlir::BoolAttr>(getProperties().narrow_range);
  }

  bool getNarrowRange();
  ::mlir::BoolAttr getIsSignedAttr() {
    return ::llvm::dyn_cast_or_null<::mlir::BoolAttr>(getProperties().is_signed);
  }

  bool getIsSigned();
  void setMinAttr(::mlir::FloatAttr attr) {
    getProperties().min = attr;
  }

  void setMin(::llvm::APFloat attrValue);
  void setMaxAttr(::mlir::FloatAttr attr) {
    getProperties().max = attr;
  }

  void setMax(::llvm::APFloat attrValue);
  void setNumBitsAttr(::mlir::IntegerAttr attr) {
    getProperties().num_bits = attr;
  }

  void setNumBits(uint64_t attrValue);
  void setNarrowRangeAttr(::mlir::BoolAttr attr) {
    getProperties().narrow_range = attr;
  }

  void setNarrowRange(::std::optional<bool> attrValue);
  void setIsSignedAttr(::mlir::BoolAttr attr) {
    getProperties().is_signed = attr;
  }

  void setIsSigned(::std::optional<bool> attrValue);
  ::mlir::Attribute removeNarrowRangeAttr() {
      auto &attr = getProperties().narrow_range;
      attr = {};
      return attr;
  }

  ::mlir::Attribute removeIsSignedAttr() {
      auto &attr = getProperties().is_signed;
      attr = {};
      return attr;
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type outputs, ::mlir::Value inputs, ::mlir::FloatAttr min, ::mlir::FloatAttr max, ::mlir::IntegerAttr num_bits, /*optional*/::mlir::BoolAttr narrow_range, /*optional*/::mlir::BoolAttr is_signed = nullptr);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value inputs, ::mlir::FloatAttr min, ::mlir::FloatAttr max, ::mlir::IntegerAttr num_bits, /*optional*/::mlir::BoolAttr narrow_range, /*optional*/::mlir::BoolAttr is_signed = nullptr);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value inputs, ::mlir::FloatAttr min, ::mlir::FloatAttr max, ::mlir::IntegerAttr num_bits, /*optional*/::mlir::BoolAttr narrow_range, /*optional*/::mlir::BoolAttr is_signed = nullptr);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type outputs, ::mlir::Value inputs, ::llvm::APFloat min, ::llvm::APFloat max, uint64_t num_bits, /*optional*/bool narrow_range = false, /*optional*/bool is_signed = false);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value inputs, ::llvm::APFloat min, ::llvm::APFloat max, uint64_t num_bits, /*optional*/bool narrow_range = false, /*optional*/bool is_signed = false);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value inputs, ::llvm::APFloat min, ::llvm::APFloat max, uint64_t num_bits, /*optional*/bool narrow_range = false, /*optional*/bool is_signed = false);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
private:
  ::mlir::StringAttr getAttributeNameForIndex(unsigned index) {
    return getAttributeNameForIndex((*this)->getName(), index);
  }

  static ::mlir::StringAttr getAttributeNameForIndex(::mlir::OperationName name, unsigned index) {
    assert(index < 5 && "invalid attribute index");
    assert(name.getStringRef() == getOperationName() && "invalid operation name");
    assert(name.isRegistered() && "Operation isn't registered, missing a "
          "dependent dialect loading?");
    return name.getAttributeNames()[index];
  }

public:
};
} // namespace quantfork
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::quantfork::ConstFakeQuant)

namespace mlir {
namespace quantfork {

//===----------------------------------------------------------------------===//
// ::mlir::quantfork::ConstFakeQuantPerAxis declarations
//===----------------------------------------------------------------------===//

namespace detail {
class ConstFakeQuantPerAxisGenericAdaptorBase {
public:
  struct Properties {
    using axisTy = ::mlir::IntegerAttr;
    axisTy axis;

    auto getAxis() {
      auto &propStorage = this->axis;
      return ::llvm::cast<::mlir::IntegerAttr>(propStorage);
    }
    void setAxis(const ::mlir::IntegerAttr &propValue) {
      this->axis = propValue;
    }
    using is_signedTy = ::mlir::BoolAttr;
    is_signedTy is_signed;

    auto getIsSigned() {
      auto &propStorage = this->is_signed;
      return ::llvm::dyn_cast_or_null<::mlir::BoolAttr>(propStorage);
    }
    void setIsSigned(const ::mlir::BoolAttr &propValue) {
      this->is_signed = propValue;
    }
    using maxTy = ::mlir::ArrayAttr;
    maxTy max;

    auto getMax() {
      auto &propStorage = this->max;
      return ::llvm::cast<::mlir::ArrayAttr>(propStorage);
    }
    void setMax(const ::mlir::ArrayAttr &propValue) {
      this->max = propValue;
    }
    using minTy = ::mlir::ArrayAttr;
    minTy min;

    auto getMin() {
      auto &propStorage = this->min;
      return ::llvm::cast<::mlir::ArrayAttr>(propStorage);
    }
    void setMin(const ::mlir::ArrayAttr &propValue) {
      this->min = propValue;
    }
    using narrow_rangeTy = ::mlir::BoolAttr;
    narrow_rangeTy narrow_range;

    auto getNarrowRange() {
      auto &propStorage = this->narrow_range;
      return ::llvm::dyn_cast_or_null<::mlir::BoolAttr>(propStorage);
    }
    void setNarrowRange(const ::mlir::BoolAttr &propValue) {
      this->narrow_range = propValue;
    }
    using num_bitsTy = ::mlir::IntegerAttr;
    num_bitsTy num_bits;

    auto getNumBits() {
      auto &propStorage = this->num_bits;
      return ::llvm::cast<::mlir::IntegerAttr>(propStorage);
    }
    void setNumBits(const ::mlir::IntegerAttr &propValue) {
      this->num_bits = propValue;
    }
    bool operator==(const Properties &rhs) const {
      return 
        rhs.axis == this->axis &&
        rhs.is_signed == this->is_signed &&
        rhs.max == this->max &&
        rhs.min == this->min &&
        rhs.narrow_range == this->narrow_range &&
        rhs.num_bits == this->num_bits &&
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
  ConstFakeQuantPerAxisGenericAdaptorBase(::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), properties(properties), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("quantfork.const_fake_quant_per_axis", odsAttrs.getContext());
  }

  ConstFakeQuantPerAxisGenericAdaptorBase(ConstFakeQuantPerAxis op);

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  const Properties &getProperties() {
    return properties;
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

  ::mlir::ArrayAttr getMinAttr() {
    auto attr = ::llvm::cast<::mlir::ArrayAttr>(getProperties().min);
    return attr;
  }

  ::mlir::ArrayAttr getMin();
  ::mlir::ArrayAttr getMaxAttr() {
    auto attr = ::llvm::cast<::mlir::ArrayAttr>(getProperties().max);
    return attr;
  }

  ::mlir::ArrayAttr getMax();
  ::mlir::IntegerAttr getAxisAttr() {
    auto attr = ::llvm::cast<::mlir::IntegerAttr>(getProperties().axis);
    return attr;
  }

  uint64_t getAxis();
  ::mlir::IntegerAttr getNumBitsAttr() {
    auto attr = ::llvm::cast<::mlir::IntegerAttr>(getProperties().num_bits);
    return attr;
  }

  uint64_t getNumBits();
  ::mlir::BoolAttr getNarrowRangeAttr();
  bool getNarrowRange();
  ::mlir::BoolAttr getIsSignedAttr();
  bool getIsSigned();
};
} // namespace detail
template <typename RangeT>
class ConstFakeQuantPerAxisGenericAdaptor : public detail::ConstFakeQuantPerAxisGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::ConstFakeQuantPerAxisGenericAdaptorBase;
public:
  ConstFakeQuantPerAxisGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  ConstFakeQuantPerAxisGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : ConstFakeQuantPerAxisGenericAdaptor(values, attrs, (properties ? *properties.as<Properties *>() : Properties{}), regions) {}

  ConstFakeQuantPerAxisGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = nullptr) : ConstFakeQuantPerAxisGenericAdaptor(values, attrs, Properties{}, {}) {}

  template <typename LateInst = ConstFakeQuantPerAxis, typename = std::enable_if_t<std::is_same_v<LateInst, ConstFakeQuantPerAxis>>>
  ConstFakeQuantPerAxisGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  ValueT getInputs() {
    return (*getODSOperands(0).begin());
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class ConstFakeQuantPerAxisAdaptor : public ConstFakeQuantPerAxisGenericAdaptor<::mlir::ValueRange> {
public:
  using ConstFakeQuantPerAxisGenericAdaptor::ConstFakeQuantPerAxisGenericAdaptor;
  ConstFakeQuantPerAxisAdaptor(ConstFakeQuantPerAxis op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class ConstFakeQuantPerAxis : public ::mlir::Op<ConstFakeQuantPerAxis, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::TensorType>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::BytecodeOpInterface::Trait, ::mlir::OpTrait::SameOperandsAndResultType, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::InferTypeOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = ConstFakeQuantPerAxisAdaptor;
  template <typename RangeT>
  using GenericAdaptor = ConstFakeQuantPerAxisGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  using Properties = FoldAdaptor::Properties;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    static ::llvm::StringRef attrNames[] = {::llvm::StringRef("axis"), ::llvm::StringRef("is_signed"), ::llvm::StringRef("max"), ::llvm::StringRef("min"), ::llvm::StringRef("narrow_range"), ::llvm::StringRef("num_bits")};
    return ::llvm::ArrayRef(attrNames);
  }

  ::mlir::StringAttr getAxisAttrName() {
    return getAttributeNameForIndex(0);
  }

  static ::mlir::StringAttr getAxisAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 0);
  }

  ::mlir::StringAttr getIsSignedAttrName() {
    return getAttributeNameForIndex(1);
  }

  static ::mlir::StringAttr getIsSignedAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 1);
  }

  ::mlir::StringAttr getMaxAttrName() {
    return getAttributeNameForIndex(2);
  }

  static ::mlir::StringAttr getMaxAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 2);
  }

  ::mlir::StringAttr getMinAttrName() {
    return getAttributeNameForIndex(3);
  }

  static ::mlir::StringAttr getMinAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 3);
  }

  ::mlir::StringAttr getNarrowRangeAttrName() {
    return getAttributeNameForIndex(4);
  }

  static ::mlir::StringAttr getNarrowRangeAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 4);
  }

  ::mlir::StringAttr getNumBitsAttrName() {
    return getAttributeNameForIndex(5);
  }

  static ::mlir::StringAttr getNumBitsAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 5);
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("quantfork.const_fake_quant_per_axis");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::TypedValue<::mlir::TensorType> getInputs() {
    return ::llvm::cast<::mlir::TypedValue<::mlir::TensorType>>(*getODSOperands(0).begin());
  }

  ::mlir::OpOperand &getInputsMutable() {
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

  ::mlir::TypedValue<::mlir::TensorType> getOutputs() {
    return ::llvm::cast<::mlir::TypedValue<::mlir::TensorType>>(*getODSResults(0).begin());
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
  ::mlir::ArrayAttr getMinAttr() {
    return ::llvm::cast<::mlir::ArrayAttr>(getProperties().min);
  }

  ::mlir::ArrayAttr getMin();
  ::mlir::ArrayAttr getMaxAttr() {
    return ::llvm::cast<::mlir::ArrayAttr>(getProperties().max);
  }

  ::mlir::ArrayAttr getMax();
  ::mlir::IntegerAttr getAxisAttr() {
    return ::llvm::cast<::mlir::IntegerAttr>(getProperties().axis);
  }

  uint64_t getAxis();
  ::mlir::IntegerAttr getNumBitsAttr() {
    return ::llvm::cast<::mlir::IntegerAttr>(getProperties().num_bits);
  }

  uint64_t getNumBits();
  ::mlir::BoolAttr getNarrowRangeAttr() {
    return ::llvm::dyn_cast_or_null<::mlir::BoolAttr>(getProperties().narrow_range);
  }

  bool getNarrowRange();
  ::mlir::BoolAttr getIsSignedAttr() {
    return ::llvm::dyn_cast_or_null<::mlir::BoolAttr>(getProperties().is_signed);
  }

  bool getIsSigned();
  void setMinAttr(::mlir::ArrayAttr attr) {
    getProperties().min = attr;
  }

  void setMaxAttr(::mlir::ArrayAttr attr) {
    getProperties().max = attr;
  }

  void setAxisAttr(::mlir::IntegerAttr attr) {
    getProperties().axis = attr;
  }

  void setAxis(uint64_t attrValue);
  void setNumBitsAttr(::mlir::IntegerAttr attr) {
    getProperties().num_bits = attr;
  }

  void setNumBits(uint64_t attrValue);
  void setNarrowRangeAttr(::mlir::BoolAttr attr) {
    getProperties().narrow_range = attr;
  }

  void setNarrowRange(::std::optional<bool> attrValue);
  void setIsSignedAttr(::mlir::BoolAttr attr) {
    getProperties().is_signed = attr;
  }

  void setIsSigned(::std::optional<bool> attrValue);
  ::mlir::Attribute removeNarrowRangeAttr() {
      auto &attr = getProperties().narrow_range;
      attr = {};
      return attr;
  }

  ::mlir::Attribute removeIsSignedAttr() {
      auto &attr = getProperties().is_signed;
      attr = {};
      return attr;
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type outputs, ::mlir::Value inputs, ::mlir::ArrayAttr min, ::mlir::ArrayAttr max, ::mlir::IntegerAttr axis, ::mlir::IntegerAttr num_bits, /*optional*/::mlir::BoolAttr narrow_range, /*optional*/::mlir::BoolAttr is_signed = nullptr);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value inputs, ::mlir::ArrayAttr min, ::mlir::ArrayAttr max, ::mlir::IntegerAttr axis, ::mlir::IntegerAttr num_bits, /*optional*/::mlir::BoolAttr narrow_range, /*optional*/::mlir::BoolAttr is_signed = nullptr);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value inputs, ::mlir::ArrayAttr min, ::mlir::ArrayAttr max, ::mlir::IntegerAttr axis, ::mlir::IntegerAttr num_bits, /*optional*/::mlir::BoolAttr narrow_range, /*optional*/::mlir::BoolAttr is_signed = nullptr);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type outputs, ::mlir::Value inputs, ::mlir::ArrayAttr min, ::mlir::ArrayAttr max, uint64_t axis, uint64_t num_bits, /*optional*/bool narrow_range = false, /*optional*/bool is_signed = false);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value inputs, ::mlir::ArrayAttr min, ::mlir::ArrayAttr max, uint64_t axis, uint64_t num_bits, /*optional*/bool narrow_range = false, /*optional*/bool is_signed = false);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value inputs, ::mlir::ArrayAttr min, ::mlir::ArrayAttr max, uint64_t axis, uint64_t num_bits, /*optional*/bool narrow_range = false, /*optional*/bool is_signed = false);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
private:
  ::mlir::StringAttr getAttributeNameForIndex(unsigned index) {
    return getAttributeNameForIndex((*this)->getName(), index);
  }

  static ::mlir::StringAttr getAttributeNameForIndex(::mlir::OperationName name, unsigned index) {
    assert(index < 6 && "invalid attribute index");
    assert(name.getStringRef() == getOperationName() && "invalid operation name");
    assert(name.isRegistered() && "Operation isn't registered, missing a "
          "dependent dialect loading?");
    return name.getAttributeNames()[index];
  }

public:
};
} // namespace quantfork
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::quantfork::ConstFakeQuantPerAxis)

namespace mlir {
namespace quantfork {

//===----------------------------------------------------------------------===//
// ::mlir::quantfork::CoupledRefOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class CoupledRefOpGenericAdaptorBase {
public:
  struct Properties {
    using coupledKeyTy = ::mlir::StringAttr;
    coupledKeyTy coupledKey;

    auto getCoupledKey() {
      auto &propStorage = this->coupledKey;
      return ::llvm::cast<::mlir::StringAttr>(propStorage);
    }
    void setCoupledKey(const ::mlir::StringAttr &propValue) {
      this->coupledKey = propValue;
    }
    bool operator==(const Properties &rhs) const {
      return 
        rhs.coupledKey == this->coupledKey &&
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
  CoupledRefOpGenericAdaptorBase(::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), properties(properties), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("quantfork.coupled_ref", odsAttrs.getContext());
  }

  CoupledRefOpGenericAdaptorBase(CoupledRefOp op);

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  const Properties &getProperties() {
    return properties;
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

  ::mlir::StringAttr getCoupledKeyAttr() {
    auto attr = ::llvm::cast<::mlir::StringAttr>(getProperties().coupledKey);
    return attr;
  }

  ::llvm::StringRef getCoupledKey();
};
} // namespace detail
template <typename RangeT>
class CoupledRefOpGenericAdaptor : public detail::CoupledRefOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::CoupledRefOpGenericAdaptorBase;
public:
  CoupledRefOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  CoupledRefOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : CoupledRefOpGenericAdaptor(values, attrs, (properties ? *properties.as<Properties *>() : Properties{}), regions) {}

  CoupledRefOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = nullptr) : CoupledRefOpGenericAdaptor(values, attrs, Properties{}, {}) {}

  template <typename LateInst = CoupledRefOp, typename = std::enable_if_t<std::is_same_v<LateInst, CoupledRefOp>>>
  CoupledRefOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

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
class CoupledRefOpAdaptor : public CoupledRefOpGenericAdaptor<::mlir::ValueRange> {
public:
  using CoupledRefOpGenericAdaptor::CoupledRefOpGenericAdaptor;
  CoupledRefOpAdaptor(CoupledRefOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class CoupledRefOp : public ::mlir::Op<CoupledRefOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::BytecodeOpInterface::Trait, ::mlir::OpTrait::SameOperandsAndResultType, ::mlir::InferTypeOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = CoupledRefOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = CoupledRefOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  using Properties = FoldAdaptor::Properties;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    static ::llvm::StringRef attrNames[] = {::llvm::StringRef("coupledKey")};
    return ::llvm::ArrayRef(attrNames);
  }

  ::mlir::StringAttr getCoupledKeyAttrName() {
    return getAttributeNameForIndex(0);
  }

  static ::mlir::StringAttr getCoupledKeyAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 0);
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("quantfork.coupled_ref");
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

  static ::mlir::LogicalResult setPropertiesFromAttr(Properties &prop, ::mlir::Attribute attr, ::llvm::function_ref<::mlir::InFlightDiagnostic()> emitError);
  static ::mlir::Attribute getPropertiesAsAttr(::mlir::MLIRContext *ctx, const Properties &prop);
  static llvm::hash_code computePropertiesHash(const Properties &prop);
  static std::optional<mlir::Attribute> getInherentAttr(::mlir::MLIRContext *ctx, const Properties &prop, llvm::StringRef name);
  static void setInherentAttr(Properties &prop, llvm::StringRef name, mlir::Attribute value);
  static void populateInherentAttrs(::mlir::MLIRContext *ctx, const Properties &prop, ::mlir::NamedAttrList &attrs);
  static ::mlir::LogicalResult verifyInherentAttrs(::mlir::OperationName opName, ::mlir::NamedAttrList &attrs, llvm::function_ref<::mlir::InFlightDiagnostic()> emitError);
  static ::mlir::LogicalResult readProperties(::mlir::DialectBytecodeReader &reader, ::mlir::OperationState &state);
  void writeProperties(::mlir::DialectBytecodeWriter &writer);
  ::mlir::StringAttr getCoupledKeyAttr() {
    return ::llvm::cast<::mlir::StringAttr>(getProperties().coupledKey);
  }

  ::llvm::StringRef getCoupledKey();
  void setCoupledKeyAttr(::mlir::StringAttr attr) {
    getProperties().coupledKey = attr;
  }

  void setCoupledKey(::llvm::StringRef attrValue);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type resultType0, ::mlir::Value arg, ::mlir::StringAttr coupledKey);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value arg, ::mlir::StringAttr coupledKey);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value arg, ::mlir::StringAttr coupledKey);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type resultType0, ::mlir::Value arg, ::llvm::StringRef coupledKey);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value arg, ::llvm::StringRef coupledKey);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value arg, ::llvm::StringRef coupledKey);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
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
} // namespace quantfork
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::quantfork::CoupledRefOp)

namespace mlir {
namespace quantfork {

//===----------------------------------------------------------------------===//
// ::mlir::quantfork::DequantizeCastOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class DequantizeCastOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  DequantizeCastOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("quantfork.dcast", odsAttrs.getContext());
  }

  DequantizeCastOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class DequantizeCastOpGenericAdaptor : public detail::DequantizeCastOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::DequantizeCastOpGenericAdaptorBase;
public:
  DequantizeCastOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  DequantizeCastOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : DequantizeCastOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = DequantizeCastOp, typename = std::enable_if_t<std::is_same_v<LateInst, DequantizeCastOp>>>
  DequantizeCastOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

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
class DequantizeCastOpAdaptor : public DequantizeCastOpGenericAdaptor<::mlir::ValueRange> {
public:
  using DequantizeCastOpGenericAdaptor::DequantizeCastOpGenericAdaptor;
  DequantizeCastOpAdaptor(DequantizeCastOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class DequantizeCastOp : public ::mlir::Op<DequantizeCastOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = DequantizeCastOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = DequantizeCastOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("quantfork.dcast");
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

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type resultType0, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace quantfork
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::quantfork::DequantizeCastOp)

namespace mlir {
namespace quantfork {

//===----------------------------------------------------------------------===//
// ::mlir::quantfork::QuantizeCastOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class QuantizeCastOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  QuantizeCastOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("quantfork.qcast", odsAttrs.getContext());
  }

  QuantizeCastOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class QuantizeCastOpGenericAdaptor : public detail::QuantizeCastOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::QuantizeCastOpGenericAdaptorBase;
public:
  QuantizeCastOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  QuantizeCastOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : QuantizeCastOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = QuantizeCastOp, typename = std::enable_if_t<std::is_same_v<LateInst, QuantizeCastOp>>>
  QuantizeCastOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

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
class QuantizeCastOpAdaptor : public QuantizeCastOpGenericAdaptor<::mlir::ValueRange> {
public:
  using QuantizeCastOpGenericAdaptor::QuantizeCastOpGenericAdaptor;
  QuantizeCastOpAdaptor(QuantizeCastOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class QuantizeCastOp : public ::mlir::Op<QuantizeCastOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = QuantizeCastOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = QuantizeCastOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("quantfork.qcast");
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

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type resultType0, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace quantfork
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::quantfork::QuantizeCastOp)

namespace mlir {
namespace quantfork {

//===----------------------------------------------------------------------===//
// ::mlir::quantfork::QuantizeRegionOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class QuantizeRegionOpGenericAdaptorBase {
public:
  struct Properties {
    using input_specsTy = ::mlir::ArrayAttr;
    input_specsTy input_specs;

    auto getInputSpecs() {
      auto &propStorage = this->input_specs;
      return ::llvm::cast<::mlir::ArrayAttr>(propStorage);
    }
    void setInputSpecs(const ::mlir::ArrayAttr &propValue) {
      this->input_specs = propValue;
    }
    using logical_kernelTy = ::mlir::StringAttr;
    logical_kernelTy logical_kernel;

    auto getLogicalKernel() {
      auto &propStorage = this->logical_kernel;
      return ::llvm::cast<::mlir::StringAttr>(propStorage);
    }
    void setLogicalKernel(const ::mlir::StringAttr &propValue) {
      this->logical_kernel = propValue;
    }
    using output_specsTy = ::mlir::ArrayAttr;
    output_specsTy output_specs;

    auto getOutputSpecs() {
      auto &propStorage = this->output_specs;
      return ::llvm::cast<::mlir::ArrayAttr>(propStorage);
    }
    void setOutputSpecs(const ::mlir::ArrayAttr &propValue) {
      this->output_specs = propValue;
    }
    bool operator==(const Properties &rhs) const {
      return 
        rhs.input_specs == this->input_specs &&
        rhs.logical_kernel == this->logical_kernel &&
        rhs.output_specs == this->output_specs &&
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
  QuantizeRegionOpGenericAdaptorBase(::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), properties(properties), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("quantfork.region", odsAttrs.getContext());
  }

  QuantizeRegionOpGenericAdaptorBase(QuantizeRegionOp op);

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize);
  const Properties &getProperties() {
    return properties;
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

  ::mlir::ArrayAttr getInputSpecsAttr() {
    auto attr = ::llvm::cast<::mlir::ArrayAttr>(getProperties().input_specs);
    return attr;
  }

  ::mlir::ArrayAttr getInputSpecs();
  ::mlir::ArrayAttr getOutputSpecsAttr() {
    auto attr = ::llvm::cast<::mlir::ArrayAttr>(getProperties().output_specs);
    return attr;
  }

  ::mlir::ArrayAttr getOutputSpecs();
  ::mlir::StringAttr getLogicalKernelAttr() {
    auto attr = ::llvm::cast<::mlir::StringAttr>(getProperties().logical_kernel);
    return attr;
  }

  ::llvm::StringRef getLogicalKernel();
  ::mlir::Region &getBody() {
    return *odsRegions[0];
  }

  ::mlir::RegionRange getRegions() {
    return odsRegions;
  }

};
} // namespace detail
template <typename RangeT>
class QuantizeRegionOpGenericAdaptor : public detail::QuantizeRegionOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::QuantizeRegionOpGenericAdaptorBase;
public:
  QuantizeRegionOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  QuantizeRegionOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : QuantizeRegionOpGenericAdaptor(values, attrs, (properties ? *properties.as<Properties *>() : Properties{}), regions) {}

  QuantizeRegionOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = nullptr) : QuantizeRegionOpGenericAdaptor(values, attrs, Properties{}, {}) {}

  template <typename LateInst = QuantizeRegionOp, typename = std::enable_if_t<std::is_same_v<LateInst, QuantizeRegionOp>>>
  QuantizeRegionOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  RangeT getInputs() {
    return getODSOperands(0);
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class QuantizeRegionOpAdaptor : public QuantizeRegionOpGenericAdaptor<::mlir::ValueRange> {
public:
  using QuantizeRegionOpGenericAdaptor::QuantizeRegionOpGenericAdaptor;
  QuantizeRegionOpAdaptor(QuantizeRegionOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class QuantizeRegionOp : public ::mlir::Op<QuantizeRegionOp, ::mlir::OpTrait::OneRegion, ::mlir::OpTrait::VariadicResults, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::VariadicOperands, ::mlir::OpTrait::SingleBlock, ::mlir::OpTrait::SingleBlockImplicitTerminator<ReturnOp>::Impl, ::mlir::OpTrait::OpInvariants, ::mlir::BytecodeOpInterface::Trait, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait, ::mlir::OpTrait::IsIsolatedFromAbove> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = QuantizeRegionOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = QuantizeRegionOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  using Properties = FoldAdaptor::Properties;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    static ::llvm::StringRef attrNames[] = {::llvm::StringRef("input_specs"), ::llvm::StringRef("logical_kernel"), ::llvm::StringRef("output_specs")};
    return ::llvm::ArrayRef(attrNames);
  }

  ::mlir::StringAttr getInputSpecsAttrName() {
    return getAttributeNameForIndex(0);
  }

  static ::mlir::StringAttr getInputSpecsAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 0);
  }

  ::mlir::StringAttr getLogicalKernelAttrName() {
    return getAttributeNameForIndex(1);
  }

  static ::mlir::StringAttr getLogicalKernelAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 1);
  }

  ::mlir::StringAttr getOutputSpecsAttrName() {
    return getAttributeNameForIndex(2);
  }

  static ::mlir::StringAttr getOutputSpecsAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 2);
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("quantfork.region");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index);
  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Operation::operand_range getInputs() {
    return getODSOperands(0);
  }

  ::mlir::MutableOperandRange getInputsMutable();
  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index);
  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Operation::result_range getOutputs() {
    return getODSResults(0);
  }

  ::mlir::Region &getBody() {
    return (*this)->getRegion(0);
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
  ::mlir::ArrayAttr getInputSpecsAttr() {
    return ::llvm::cast<::mlir::ArrayAttr>(getProperties().input_specs);
  }

  ::mlir::ArrayAttr getInputSpecs();
  ::mlir::ArrayAttr getOutputSpecsAttr() {
    return ::llvm::cast<::mlir::ArrayAttr>(getProperties().output_specs);
  }

  ::mlir::ArrayAttr getOutputSpecs();
  ::mlir::StringAttr getLogicalKernelAttr() {
    return ::llvm::cast<::mlir::StringAttr>(getProperties().logical_kernel);
  }

  ::llvm::StringRef getLogicalKernel();
  void setInputSpecsAttr(::mlir::ArrayAttr attr) {
    getProperties().input_specs = attr;
  }

  void setOutputSpecsAttr(::mlir::ArrayAttr attr) {
    getProperties().output_specs = attr;
  }

  void setLogicalKernelAttr(::mlir::StringAttr attr) {
    getProperties().logical_kernel = attr;
  }

  void setLogicalKernel(::llvm::StringRef attrValue);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange outputs, ::mlir::ValueRange inputs, ::mlir::ArrayAttr input_specs, ::mlir::ArrayAttr output_specs, ::mlir::StringAttr logical_kernel);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange outputs, ::mlir::ValueRange inputs, ::mlir::ArrayAttr input_specs, ::mlir::ArrayAttr output_specs, ::llvm::StringRef logical_kernel);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  ::mlir::LogicalResult verify();
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
private:
  ::mlir::StringAttr getAttributeNameForIndex(unsigned index) {
    return getAttributeNameForIndex((*this)->getName(), index);
  }

  static ::mlir::StringAttr getAttributeNameForIndex(::mlir::OperationName name, unsigned index) {
    assert(index < 3 && "invalid attribute index");
    assert(name.getStringRef() == getOperationName() && "invalid operation name");
    assert(name.isRegistered() && "Operation isn't registered, missing a "
          "dependent dialect loading?");
    return name.getAttributeNames()[index];
  }

public:
};
} // namespace quantfork
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::quantfork::QuantizeRegionOp)

namespace mlir {
namespace quantfork {

//===----------------------------------------------------------------------===//
// ::mlir::quantfork::ReturnOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class ReturnOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  ReturnOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("quantfork.return", odsAttrs.getContext());
  }

  ReturnOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize);
  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class ReturnOpGenericAdaptor : public detail::ReturnOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::ReturnOpGenericAdaptorBase;
public:
  ReturnOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  ReturnOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : ReturnOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = ReturnOp, typename = std::enable_if_t<std::is_same_v<LateInst, ReturnOp>>>
  ReturnOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index) {
    return Base::getODSOperandIndexAndLength(index, odsOperands.size());
  }

  RangeT getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(odsOperands.begin(), valueRange.first),
             std::next(odsOperands.begin(), valueRange.first + valueRange.second)};
  }

  RangeT getResults() {
    return getODSOperands(0);
  }

  RangeT getOperands() {
    return odsOperands;
  }

private:
  RangeT odsOperands;
};
class ReturnOpAdaptor : public ReturnOpGenericAdaptor<::mlir::ValueRange> {
public:
  using ReturnOpGenericAdaptor::ReturnOpGenericAdaptor;
  ReturnOpAdaptor(ReturnOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class ReturnOp : public ::mlir::Op<ReturnOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::ZeroResults, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::VariadicOperands, ::mlir::OpTrait::OpInvariants, ::mlir::OpTrait::IsTerminator> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = ReturnOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = ReturnOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("quantfork.return");
  }

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index);
  ::mlir::Operation::operand_range getODSOperands(unsigned index) {
    auto valueRange = getODSOperandIndexAndLength(index);
    return {std::next(getOperation()->operand_begin(), valueRange.first),
             std::next(getOperation()->operand_begin(), valueRange.first + valueRange.second)};
  }

  ::mlir::Operation::operand_range getResults() {
    return getODSOperands(0);
  }

  ::mlir::MutableOperandRange getResultsMutable();
  std::pair<unsigned, unsigned> getODSResultIndexAndLength(unsigned index) {
    return {index, 1};
  }

  ::mlir::Operation::result_range getODSResults(unsigned index) {
    auto valueRange = getODSResultIndexAndLength(index);
    return {std::next(getOperation()->result_begin(), valueRange.first),
             std::next(getOperation()->result_begin(), valueRange.first + valueRange.second)};
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange results);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
public:
};
} // namespace quantfork
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::quantfork::ReturnOp)

namespace mlir {
namespace quantfork {

//===----------------------------------------------------------------------===//
// ::mlir::quantfork::StatisticsOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class StatisticsOpGenericAdaptorBase {
public:
  struct Properties {
    using axisTy = ::mlir::IntegerAttr;
    axisTy axis;

    auto getAxis() {
      auto &propStorage = this->axis;
      return ::llvm::dyn_cast_or_null<::mlir::IntegerAttr>(propStorage);
    }
    void setAxis(const ::mlir::IntegerAttr &propValue) {
      this->axis = propValue;
    }
    using axisStatsTy = ::mlir::ElementsAttr;
    axisStatsTy axisStats;

    auto getAxisStats() {
      auto &propStorage = this->axisStats;
      return ::llvm::dyn_cast_or_null<::mlir::ElementsAttr>(propStorage);
    }
    void setAxisStats(const ::mlir::ElementsAttr &propValue) {
      this->axisStats = propValue;
    }
    using layerStatsTy = ::mlir::ElementsAttr;
    layerStatsTy layerStats;

    auto getLayerStats() {
      auto &propStorage = this->layerStats;
      return ::llvm::cast<::mlir::ElementsAttr>(propStorage);
    }
    void setLayerStats(const ::mlir::ElementsAttr &propValue) {
      this->layerStats = propValue;
    }
    bool operator==(const Properties &rhs) const {
      return 
        rhs.axis == this->axis &&
        rhs.axisStats == this->axisStats &&
        rhs.layerStats == this->layerStats &&
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
  StatisticsOpGenericAdaptorBase(::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), properties(properties), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("quantfork.stats", odsAttrs.getContext());
  }

  StatisticsOpGenericAdaptorBase(StatisticsOp op);

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  const Properties &getProperties() {
    return properties;
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

  ::mlir::ElementsAttr getLayerStatsAttr() {
    auto attr = ::llvm::cast<::mlir::ElementsAttr>(getProperties().layerStats);
    return attr;
  }

  ::mlir::ElementsAttr getLayerStats();
  ::mlir::ElementsAttr getAxisStatsAttr() {
    auto attr = ::llvm::dyn_cast_or_null<::mlir::ElementsAttr>(getProperties().axisStats);
    return attr;
  }

  ::std::optional< ::mlir::ElementsAttr > getAxisStats();
  ::mlir::IntegerAttr getAxisAttr() {
    auto attr = ::llvm::dyn_cast_or_null<::mlir::IntegerAttr>(getProperties().axis);
    return attr;
  }

  ::std::optional<uint64_t> getAxis();
};
} // namespace detail
template <typename RangeT>
class StatisticsOpGenericAdaptor : public detail::StatisticsOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::StatisticsOpGenericAdaptorBase;
public:
  StatisticsOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  StatisticsOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : StatisticsOpGenericAdaptor(values, attrs, (properties ? *properties.as<Properties *>() : Properties{}), regions) {}

  StatisticsOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = nullptr) : StatisticsOpGenericAdaptor(values, attrs, Properties{}, {}) {}

  template <typename LateInst = StatisticsOp, typename = std::enable_if_t<std::is_same_v<LateInst, StatisticsOp>>>
  StatisticsOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

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
class StatisticsOpAdaptor : public StatisticsOpGenericAdaptor<::mlir::ValueRange> {
public:
  using StatisticsOpGenericAdaptor::StatisticsOpGenericAdaptor;
  StatisticsOpAdaptor(StatisticsOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class StatisticsOp : public ::mlir::Op<StatisticsOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::BytecodeOpInterface::Trait, ::mlir::OpTrait::SameOperandsAndResultType, ::mlir::InferTypeOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = StatisticsOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = StatisticsOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  using Properties = FoldAdaptor::Properties;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    static ::llvm::StringRef attrNames[] = {::llvm::StringRef("axis"), ::llvm::StringRef("axisStats"), ::llvm::StringRef("layerStats")};
    return ::llvm::ArrayRef(attrNames);
  }

  ::mlir::StringAttr getAxisAttrName() {
    return getAttributeNameForIndex(0);
  }

  static ::mlir::StringAttr getAxisAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 0);
  }

  ::mlir::StringAttr getAxisStatsAttrName() {
    return getAttributeNameForIndex(1);
  }

  static ::mlir::StringAttr getAxisStatsAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 1);
  }

  ::mlir::StringAttr getLayerStatsAttrName() {
    return getAttributeNameForIndex(2);
  }

  static ::mlir::StringAttr getLayerStatsAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 2);
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("quantfork.stats");
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

  static ::mlir::LogicalResult setPropertiesFromAttr(Properties &prop, ::mlir::Attribute attr, ::llvm::function_ref<::mlir::InFlightDiagnostic()> emitError);
  static ::mlir::Attribute getPropertiesAsAttr(::mlir::MLIRContext *ctx, const Properties &prop);
  static llvm::hash_code computePropertiesHash(const Properties &prop);
  static std::optional<mlir::Attribute> getInherentAttr(::mlir::MLIRContext *ctx, const Properties &prop, llvm::StringRef name);
  static void setInherentAttr(Properties &prop, llvm::StringRef name, mlir::Attribute value);
  static void populateInherentAttrs(::mlir::MLIRContext *ctx, const Properties &prop, ::mlir::NamedAttrList &attrs);
  static ::mlir::LogicalResult verifyInherentAttrs(::mlir::OperationName opName, ::mlir::NamedAttrList &attrs, llvm::function_ref<::mlir::InFlightDiagnostic()> emitError);
  static ::mlir::LogicalResult readProperties(::mlir::DialectBytecodeReader &reader, ::mlir::OperationState &state);
  void writeProperties(::mlir::DialectBytecodeWriter &writer);
  ::mlir::ElementsAttr getLayerStatsAttr() {
    return ::llvm::cast<::mlir::ElementsAttr>(getProperties().layerStats);
  }

  ::mlir::ElementsAttr getLayerStats();
  ::mlir::ElementsAttr getAxisStatsAttr() {
    return ::llvm::dyn_cast_or_null<::mlir::ElementsAttr>(getProperties().axisStats);
  }

  ::std::optional< ::mlir::ElementsAttr > getAxisStats();
  ::mlir::IntegerAttr getAxisAttr() {
    return ::llvm::dyn_cast_or_null<::mlir::IntegerAttr>(getProperties().axis);
  }

  ::std::optional<uint64_t> getAxis();
  void setLayerStatsAttr(::mlir::ElementsAttr attr) {
    getProperties().layerStats = attr;
  }

  void setAxisStatsAttr(::mlir::ElementsAttr attr) {
    getProperties().axisStats = attr;
  }

  void setAxisAttr(::mlir::IntegerAttr attr) {
    getProperties().axis = attr;
  }

  void setAxis(::std::optional<uint64_t> attrValue);
  ::mlir::Attribute removeAxisStatsAttr() {
      auto &attr = getProperties().axisStats;
      attr = {};
      return attr;
  }

  ::mlir::Attribute removeAxisAttr() {
      auto &attr = getProperties().axis;
      attr = {};
      return attr;
  }

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type resultType0, ::mlir::Value arg, ::mlir::ElementsAttr layerStats, /*optional*/::mlir::ElementsAttr axisStats, /*optional*/::mlir::IntegerAttr axis);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value arg, ::mlir::ElementsAttr layerStats, /*optional*/::mlir::ElementsAttr axisStats, /*optional*/::mlir::IntegerAttr axis);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value arg, ::mlir::ElementsAttr layerStats, /*optional*/::mlir::ElementsAttr axisStats, /*optional*/::mlir::IntegerAttr axis);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  ::mlir::LogicalResult verify();
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
private:
  ::mlir::StringAttr getAttributeNameForIndex(unsigned index) {
    return getAttributeNameForIndex((*this)->getName(), index);
  }

  static ::mlir::StringAttr getAttributeNameForIndex(::mlir::OperationName name, unsigned index) {
    assert(index < 3 && "invalid attribute index");
    assert(name.getStringRef() == getOperationName() && "invalid operation name");
    assert(name.isRegistered() && "Operation isn't registered, missing a "
          "dependent dialect loading?");
    return name.getAttributeNames()[index];
  }

public:
};
} // namespace quantfork
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::quantfork::StatisticsOp)

namespace mlir {
namespace quantfork {

//===----------------------------------------------------------------------===//
// ::mlir::quantfork::StatisticsRefOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class StatisticsRefOpGenericAdaptorBase {
public:
  struct Properties {
    using statsKeyTy = ::mlir::StringAttr;
    statsKeyTy statsKey;

    auto getStatsKey() {
      auto &propStorage = this->statsKey;
      return ::llvm::cast<::mlir::StringAttr>(propStorage);
    }
    void setStatsKey(const ::mlir::StringAttr &propValue) {
      this->statsKey = propValue;
    }
    bool operator==(const Properties &rhs) const {
      return 
        rhs.statsKey == this->statsKey &&
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
  StatisticsRefOpGenericAdaptorBase(::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), properties(properties), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("quantfork.stats_ref", odsAttrs.getContext());
  }

  StatisticsRefOpGenericAdaptorBase(StatisticsRefOp op);

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  const Properties &getProperties() {
    return properties;
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

  ::mlir::StringAttr getStatsKeyAttr() {
    auto attr = ::llvm::cast<::mlir::StringAttr>(getProperties().statsKey);
    return attr;
  }

  ::llvm::StringRef getStatsKey();
};
} // namespace detail
template <typename RangeT>
class StatisticsRefOpGenericAdaptor : public detail::StatisticsRefOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::StatisticsRefOpGenericAdaptorBase;
public:
  StatisticsRefOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, const Properties &properties, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  StatisticsRefOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : StatisticsRefOpGenericAdaptor(values, attrs, (properties ? *properties.as<Properties *>() : Properties{}), regions) {}

  StatisticsRefOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = nullptr) : StatisticsRefOpGenericAdaptor(values, attrs, Properties{}, {}) {}

  template <typename LateInst = StatisticsRefOp, typename = std::enable_if_t<std::is_same_v<LateInst, StatisticsRefOp>>>
  StatisticsRefOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

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
class StatisticsRefOpAdaptor : public StatisticsRefOpGenericAdaptor<::mlir::ValueRange> {
public:
  using StatisticsRefOpGenericAdaptor::StatisticsRefOpGenericAdaptor;
  StatisticsRefOpAdaptor(StatisticsRefOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class StatisticsRefOp : public ::mlir::Op<StatisticsRefOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::BytecodeOpInterface::Trait, ::mlir::OpTrait::SameOperandsAndResultType, ::mlir::InferTypeOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = StatisticsRefOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = StatisticsRefOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  using Properties = FoldAdaptor::Properties;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    static ::llvm::StringRef attrNames[] = {::llvm::StringRef("statsKey")};
    return ::llvm::ArrayRef(attrNames);
  }

  ::mlir::StringAttr getStatsKeyAttrName() {
    return getAttributeNameForIndex(0);
  }

  static ::mlir::StringAttr getStatsKeyAttrName(::mlir::OperationName name) {
    return getAttributeNameForIndex(name, 0);
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("quantfork.stats_ref");
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

  static ::mlir::LogicalResult setPropertiesFromAttr(Properties &prop, ::mlir::Attribute attr, ::llvm::function_ref<::mlir::InFlightDiagnostic()> emitError);
  static ::mlir::Attribute getPropertiesAsAttr(::mlir::MLIRContext *ctx, const Properties &prop);
  static llvm::hash_code computePropertiesHash(const Properties &prop);
  static std::optional<mlir::Attribute> getInherentAttr(::mlir::MLIRContext *ctx, const Properties &prop, llvm::StringRef name);
  static void setInherentAttr(Properties &prop, llvm::StringRef name, mlir::Attribute value);
  static void populateInherentAttrs(::mlir::MLIRContext *ctx, const Properties &prop, ::mlir::NamedAttrList &attrs);
  static ::mlir::LogicalResult verifyInherentAttrs(::mlir::OperationName opName, ::mlir::NamedAttrList &attrs, llvm::function_ref<::mlir::InFlightDiagnostic()> emitError);
  static ::mlir::LogicalResult readProperties(::mlir::DialectBytecodeReader &reader, ::mlir::OperationState &state);
  void writeProperties(::mlir::DialectBytecodeWriter &writer);
  ::mlir::StringAttr getStatsKeyAttr() {
    return ::llvm::cast<::mlir::StringAttr>(getProperties().statsKey);
  }

  ::llvm::StringRef getStatsKey();
  void setStatsKeyAttr(::mlir::StringAttr attr) {
    getProperties().statsKey = attr;
  }

  void setStatsKey(::llvm::StringRef attrValue);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type resultType0, ::mlir::Value arg, ::mlir::StringAttr statsKey);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value arg, ::mlir::StringAttr statsKey);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value arg, ::mlir::StringAttr statsKey);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type resultType0, ::mlir::Value arg, ::llvm::StringRef statsKey);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Value arg, ::llvm::StringRef statsKey);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value arg, ::llvm::StringRef statsKey);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  static ::mlir::LogicalResult inferReturnTypes(::mlir::MLIRContext *context, ::std::optional<::mlir::Location> location, ::mlir::ValueRange operands, ::mlir::DictionaryAttr attributes, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions, ::llvm::SmallVectorImpl<::mlir::Type>&inferredReturnTypes);
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
} // namespace quantfork
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::quantfork::StatisticsRefOp)

namespace mlir {
namespace quantfork {

//===----------------------------------------------------------------------===//
// ::mlir::quantfork::StorageCastOp declarations
//===----------------------------------------------------------------------===//

namespace detail {
class StorageCastOpGenericAdaptorBase {
public:
protected:
  ::mlir::DictionaryAttr odsAttrs;
  ::std::optional<::mlir::OperationName> odsOpName;
  ::mlir::RegionRange odsRegions;
public:
  StorageCastOpGenericAdaptorBase(::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : odsAttrs(attrs), odsRegions(regions) {  if (odsAttrs)
      odsOpName.emplace("quantfork.scast", odsAttrs.getContext());
  }

  StorageCastOpGenericAdaptorBase(::mlir::Operation *op) : odsAttrs(op->getRawDictionaryAttrs()), odsOpName(op->getName()), odsRegions(op->getRegions()) {}

  std::pair<unsigned, unsigned> getODSOperandIndexAndLength(unsigned index, unsigned odsOperandsSize) {
    return {index, 1};
  }

  ::mlir::DictionaryAttr getAttributes() {
    return odsAttrs;
  }

};
} // namespace detail
template <typename RangeT>
class StorageCastOpGenericAdaptor : public detail::StorageCastOpGenericAdaptorBase {
  using ValueT = ::llvm::detail::ValueOfRange<RangeT>;
  using Base = detail::StorageCastOpGenericAdaptorBase;
public:
  StorageCastOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs = {}, const ::mlir::EmptyProperties &properties = {}, ::mlir::RegionRange regions = {}) : Base(attrs, properties, regions), odsOperands(values) {}

  StorageCastOpGenericAdaptor(RangeT values, ::mlir::DictionaryAttr attrs, ::mlir::OpaqueProperties properties, ::mlir::RegionRange regions = {}) : StorageCastOpGenericAdaptor(values, attrs, (properties ? *properties.as<::mlir::EmptyProperties *>() : ::mlir::EmptyProperties{}), regions) {}

  template <typename LateInst = StorageCastOp, typename = std::enable_if_t<std::is_same_v<LateInst, StorageCastOp>>>
  StorageCastOpGenericAdaptor(RangeT values, LateInst op) : Base(op), odsOperands(values) {}

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
class StorageCastOpAdaptor : public StorageCastOpGenericAdaptor<::mlir::ValueRange> {
public:
  using StorageCastOpGenericAdaptor::StorageCastOpGenericAdaptor;
  StorageCastOpAdaptor(StorageCastOp op);

  ::mlir::LogicalResult verify(::mlir::Location loc);
};
class StorageCastOp : public ::mlir::Op<StorageCastOp, ::mlir::OpTrait::ZeroRegions, ::mlir::OpTrait::OneResult, ::mlir::OpTrait::OneTypedResult<::mlir::Type>::Impl, ::mlir::OpTrait::ZeroSuccessors, ::mlir::OpTrait::OneOperand, ::mlir::OpTrait::OpInvariants, ::mlir::ConditionallySpeculatable::Trait, ::mlir::OpTrait::AlwaysSpeculatableImplTrait, ::mlir::MemoryEffectOpInterface::Trait> {
public:
  using Op::Op;
  using Op::print;
  using Adaptor = StorageCastOpAdaptor;
  template <typename RangeT>
  using GenericAdaptor = StorageCastOpGenericAdaptor<RangeT>;
  using FoldAdaptor = GenericAdaptor<::llvm::ArrayRef<::mlir::Attribute>>;
  static ::llvm::ArrayRef<::llvm::StringRef> getAttributeNames() {
    return {};
  }

  static constexpr ::llvm::StringLiteral getOperationName() {
    return ::llvm::StringLiteral("quantfork.scast");
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

  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::Type resultType0, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &odsBuilder, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::Value arg);
  static void build(::mlir::OpBuilder &, ::mlir::OperationState &odsState, ::mlir::TypeRange resultTypes, ::mlir::ValueRange operands, ::llvm::ArrayRef<::mlir::NamedAttribute> attributes = {});
  ::mlir::LogicalResult verifyInvariantsImpl();
  ::mlir::LogicalResult verifyInvariants();
  ::mlir::OpFoldResult fold(FoldAdaptor adaptor);
  void getEffects(::llvm::SmallVectorImpl<::mlir::SideEffects::EffectInstance<::mlir::MemoryEffects::Effect>> &effects);
public:
};
} // namespace quantfork
} // namespace mlir
MLIR_DECLARE_EXPLICIT_TYPE_ID(::mlir::quantfork::StorageCastOp)


#endif  // GET_OP_CLASSES

