/*===- TableGen'erated file -------------------------------------*- C++ -*-===*\
|*                                                                            *|
|* AttrDef Definitions                                                        *|
|*                                                                            *|
|* Automatically generated file, do not edit!                                 *|
|*                                                                            *|
\*===----------------------------------------------------------------------===*/

#ifdef GET_ATTRDEF_LIST
#undef GET_ATTRDEF_LIST

::mlir::mesh::ReductionKindAttr,
::mlir::mesh::MeshShardingAttr

#endif  // GET_ATTRDEF_LIST

#ifdef GET_ATTRDEF_CLASSES
#undef GET_ATTRDEF_CLASSES

static ::mlir::OptionalParseResult generatedAttributeParser(::mlir::AsmParser &parser, ::llvm::StringRef *mnemonic, ::mlir::Type type, ::mlir::Attribute &value) {
  return ::mlir::AsmParser::KeywordSwitch<::mlir::OptionalParseResult>(parser)
    .Case(::mlir::mesh::ReductionKindAttr::getMnemonic(), [&](llvm::StringRef, llvm::SMLoc) {
      value = ::mlir::mesh::ReductionKindAttr::parse(parser, type);
      return ::mlir::success(!!value);
    })
    .Case(::mlir::mesh::MeshShardingAttr::getMnemonic(), [&](llvm::StringRef, llvm::SMLoc) {
      value = ::mlir::mesh::MeshShardingAttr::parse(parser, type);
      return ::mlir::success(!!value);
    })
    .Default([&](llvm::StringRef keyword, llvm::SMLoc) {
      *mnemonic = keyword;
      return std::nullopt;
    });
}

static ::mlir::LogicalResult generatedAttributePrinter(::mlir::Attribute def, ::mlir::AsmPrinter &printer) {
  return ::llvm::TypeSwitch<::mlir::Attribute, ::mlir::LogicalResult>(def)    .Case<::mlir::mesh::ReductionKindAttr>([&](auto t) {
      printer << ::mlir::mesh::ReductionKindAttr::getMnemonic();
t.print(printer);
      return ::mlir::success();
    })
    .Case<::mlir::mesh::MeshShardingAttr>([&](auto t) {
      printer << ::mlir::mesh::MeshShardingAttr::getMnemonic();
t.print(printer);
      return ::mlir::success();
    })
    .Default([](auto) { return ::mlir::failure(); });
}

namespace mlir {
namespace mesh {
namespace detail {
struct ReductionKindAttrStorage : public ::mlir::AttributeStorage {
  using KeyTy = std::tuple<::mlir::mesh::ReductionKind>;
  ReductionKindAttrStorage(::mlir::mesh::ReductionKind value) : value(std::move(value)) {}

  KeyTy getAsKey() const {
    return KeyTy(value);
  }

  bool operator==(const KeyTy &tblgenKey) const {
    return (value == std::get<0>(tblgenKey));
  }

  static ::llvm::hash_code hashKey(const KeyTy &tblgenKey) {
    return ::llvm::hash_combine(std::get<0>(tblgenKey));
  }

  static ReductionKindAttrStorage *construct(::mlir::AttributeStorageAllocator &allocator, KeyTy &&tblgenKey) {
    auto value = std::move(std::get<0>(tblgenKey));
    return new (allocator.allocate<ReductionKindAttrStorage>()) ReductionKindAttrStorage(std::move(value));
  }

  ::mlir::mesh::ReductionKind value;
};
} // namespace detail
ReductionKindAttr ReductionKindAttr::get(::mlir::MLIRContext *context, ::mlir::mesh::ReductionKind value) {
  return Base::get(context, std::move(value));
}

::mlir::Attribute ReductionKindAttr::parse(::mlir::AsmParser &odsParser, ::mlir::Type odsType) {
  ::mlir::Builder odsBuilder(odsParser.getContext());
  ::llvm::SMLoc odsLoc = odsParser.getCurrentLocation();
  (void) odsLoc;
  ::mlir::FailureOr<::mlir::mesh::ReductionKind> _result_value;
  // Parse literal '<'
  if (odsParser.parseLess()) return {};

  // Parse variable 'value'
  _result_value = [&]() -> ::mlir::FailureOr<::mlir::mesh::ReductionKind> {
      auto loc = odsParser.getCurrentLocation();
      ::llvm::StringRef enumKeyword;
      if (::mlir::failed(odsParser.parseKeyword(&enumKeyword)))
        return ::mlir::failure();
      auto maybeEnum = ::mlir::mesh::symbolizeReductionKind(enumKeyword);
      if (maybeEnum)
        return *maybeEnum;
      return {(::mlir::LogicalResult)(odsParser.emitError(loc) << "expected " << "::mlir::mesh::ReductionKind" << " to be one of: " << "sum" << ", " << "max" << ", " << "min" << ", " << "product" << ", " << "average" << ", " << "bitwise_and" << ", " << "bitwise_or" << ", " << "bitwise_xor" << ", " << "generic")};
    }();
  if (::mlir::failed(_result_value)) {
    odsParser.emitError(odsParser.getCurrentLocation(), "failed to parse Mesh_ReductionKindAttr parameter 'value' which is to be a `::mlir::mesh::ReductionKind`");
    return {};
  }
  // Parse literal '>'
  if (odsParser.parseGreater()) return {};
  assert(::mlir::succeeded(_result_value));
  return ReductionKindAttr::get(odsParser.getContext(),
      ::mlir::mesh::ReductionKind((*_result_value)));
}

void ReductionKindAttr::print(::mlir::AsmPrinter &odsPrinter) const {
  ::mlir::Builder odsBuilder(getContext());
  odsPrinter << "<";
  odsPrinter << stringifyReductionKind(getValue());
  odsPrinter << ">";
}

::mlir::mesh::ReductionKind ReductionKindAttr::getValue() const {
  return getImpl()->value;
}

} // namespace mesh
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::mesh::ReductionKindAttr)
namespace mlir {
namespace mesh {
namespace detail {
struct MeshShardingAttrStorage : public ::mlir::AttributeStorage {
  using KeyTy = std::tuple<::mlir::FlatSymbolRefAttr, ::llvm::ArrayRef<MeshAxesAttr>, ::llvm::ArrayRef<MeshAxis>, ::mlir::mesh::ReductionKind>;
  MeshShardingAttrStorage(::mlir::FlatSymbolRefAttr mesh, ::llvm::ArrayRef<MeshAxesAttr> split_axes, ::llvm::ArrayRef<MeshAxis> partial_axes, ::mlir::mesh::ReductionKind partial_type) : mesh(std::move(mesh)), split_axes(std::move(split_axes)), partial_axes(std::move(partial_axes)), partial_type(std::move(partial_type)) {}

  KeyTy getAsKey() const {
    return KeyTy(mesh, split_axes, partial_axes, partial_type);
  }

  bool operator==(const KeyTy &tblgenKey) const {
    return (mesh == std::get<0>(tblgenKey)) && (split_axes == std::get<1>(tblgenKey)) && (::llvm::ArrayRef<MeshAxis>(partial_axes) == ::llvm::ArrayRef<MeshAxis>(std::get<2>(tblgenKey))) && (partial_type == std::get<3>(tblgenKey));
  }

  static ::llvm::hash_code hashKey(const KeyTy &tblgenKey) {
    return ::llvm::hash_combine(std::get<0>(tblgenKey), std::get<1>(tblgenKey), std::get<2>(tblgenKey), std::get<3>(tblgenKey));
  }

  static MeshShardingAttrStorage *construct(::mlir::AttributeStorageAllocator &allocator, KeyTy &&tblgenKey) {
    auto mesh = std::move(std::get<0>(tblgenKey));
    auto split_axes = std::move(std::get<1>(tblgenKey));
    auto partial_axes = std::move(std::get<2>(tblgenKey));
    auto partial_type = std::move(std::get<3>(tblgenKey));
    split_axes = allocator.copyInto(split_axes);
    partial_axes = allocator.copyInto(partial_axes);
    return new (allocator.allocate<MeshShardingAttrStorage>()) MeshShardingAttrStorage(std::move(mesh), std::move(split_axes), std::move(partial_axes), std::move(partial_type));
  }

  ::mlir::FlatSymbolRefAttr mesh;
  ::llvm::ArrayRef<MeshAxesAttr> split_axes;
  ::llvm::ArrayRef<MeshAxis> partial_axes;
  ::mlir::mesh::ReductionKind partial_type;
};
} // namespace detail
MeshShardingAttr MeshShardingAttr::get(::mlir::MLIRContext *context, ::mlir::FlatSymbolRefAttr mesh, ::llvm::ArrayRef<MeshAxesAttr> split_axes, ::llvm::ArrayRef<MeshAxis> partial_axes, ::mlir::mesh::ReductionKind partial_type) {
  return Base::get(context, std::move(mesh), std::move(split_axes), std::move(partial_axes), std::move(partial_type));
}

MeshShardingAttr MeshShardingAttr::getChecked(::llvm::function_ref<::mlir::InFlightDiagnostic()> emitError, ::mlir::MLIRContext *context, ::mlir::FlatSymbolRefAttr mesh, ::llvm::ArrayRef<MeshAxesAttr> split_axes, ::llvm::ArrayRef<MeshAxis> partial_axes, ::mlir::mesh::ReductionKind partial_type) {
  return Base::getChecked(emitError, context, mesh, split_axes, partial_axes, partial_type);
}

MeshShardingAttr MeshShardingAttr::get(::mlir::MLIRContext *context, FlatSymbolRefAttr mesh, ArrayRef<SmallVector<MeshAxis>> split_axes, ArrayRef<MeshAxis> partial_axes, mesh::ReductionKind partial_type) {
  SmallVector<MeshAxesAttr> splitAxesAttr = llvm::map_to_vector(
              split_axes, [&](ArrayRef<MeshAxis> array) {
      return MeshAxesAttr::get(context, array);
  });
  return Base::get(context, mesh, splitAxesAttr, partial_axes,
               partial_type);
}

MeshShardingAttr MeshShardingAttr::getChecked(::llvm::function_ref<::mlir::InFlightDiagnostic()> emitError, ::mlir::MLIRContext *context, FlatSymbolRefAttr mesh, ArrayRef<SmallVector<MeshAxis>> split_axes, ArrayRef<MeshAxis> partial_axes, mesh::ReductionKind partial_type) {
  SmallVector<MeshAxesAttr> splitAxesAttr = llvm::map_to_vector(
              split_axes, [&](ArrayRef<MeshAxis> array) {
      return MeshAxesAttr::get(context, array);
  });
  return Base::getChecked(emitError, context, mesh, splitAxesAttr, partial_axes,
               partial_type);
}

MeshShardingAttr MeshShardingAttr::get(::mlir::MLIRContext *context, FlatSymbolRefAttr mesh, ArrayRef<SmallVector<MeshAxis>> split_axes) {
  return MeshShardingAttr::get(context, mesh, split_axes, {}, ReductionKind::Sum);
}

MeshShardingAttr MeshShardingAttr::getChecked(::llvm::function_ref<::mlir::InFlightDiagnostic()> emitError, ::mlir::MLIRContext *context, FlatSymbolRefAttr mesh, ArrayRef<SmallVector<MeshAxis>> split_axes) {
  return MeshShardingAttr::get(context, mesh, split_axes, {}, ReductionKind::Sum);
}

::mlir::Attribute MeshShardingAttr::parse(::mlir::AsmParser &odsParser, ::mlir::Type odsType) {
  ::mlir::Builder odsBuilder(odsParser.getContext());
  ::llvm::SMLoc odsLoc = odsParser.getCurrentLocation();
  (void) odsLoc;
  ::mlir::FailureOr<::mlir::FlatSymbolRefAttr> _result_mesh;
  ::mlir::FailureOr<::llvm::SmallVector<MeshAxesAttr>> _result_split_axes;
  ::mlir::FailureOr<::llvm::SmallVector<MeshAxis>> _result_partial_axes;
  ::mlir::FailureOr<::mlir::mesh::ReductionKind> _result_partial_type;
  // Parse literal '<'
  if (odsParser.parseLess()) return {};

  // Parse variable 'mesh'
  _result_mesh = ::mlir::FieldParser<::mlir::FlatSymbolRefAttr>::parse(odsParser);
  if (::mlir::failed(_result_mesh)) {
    odsParser.emitError(odsParser.getCurrentLocation(), "failed to parse MeshSharding parameter 'mesh' which is to be a `::mlir::FlatSymbolRefAttr`");
    return {};
  }
  // Parse literal ','
  if (odsParser.parseComma()) return {};
  // Parse literal '['
  if (odsParser.parseLSquare()) return {};

  // Parse variable 'split_axes'
  _result_split_axes = ::mlir::FieldParser<::llvm::SmallVector<MeshAxesAttr>>::parse(odsParser);
  if (::mlir::failed(_result_split_axes)) {
    odsParser.emitError(odsParser.getCurrentLocation(), "failed to parse MeshSharding parameter 'split_axes' which is to be a `::llvm::ArrayRef<MeshAxesAttr>`");
    return {};
  }
  // Parse literal ']'
  if (odsParser.parseRSquare()) return {};
  // Parse literal ','
  if (odsParser.parseOptionalComma()) {
  } else {
    // Parse literal 'partial'
    if (odsParser.parseKeyword("partial")) return {};
    // Parse literal '='
    if (odsParser.parseEqual()) return {};

    // Parse variable 'partial_type'
    _result_partial_type = ::mlir::FieldParser<::mlir::mesh::ReductionKind>::parse(odsParser);
    if (::mlir::failed(_result_partial_type)) {
      odsParser.emitError(odsParser.getCurrentLocation(), "failed to parse MeshSharding parameter 'partial_type' which is to be a `::mlir::mesh::ReductionKind`");
      return {};
    }
    // Parse literal '['
    if (odsParser.parseLSquare()) return {};

    // Parse variable 'partial_axes'
    _result_partial_axes = ::mlir::FieldParser<::llvm::SmallVector<MeshAxis>>::parse(odsParser);
    if (::mlir::failed(_result_partial_axes)) {
      odsParser.emitError(odsParser.getCurrentLocation(), "failed to parse MeshSharding parameter 'partial_axes' which is to be a `::llvm::ArrayRef<MeshAxis>`");
      return {};
    }
    // Parse literal ']'
    if (odsParser.parseRSquare()) return {};
  }
  // Parse literal '>'
  if (odsParser.parseGreater()) return {};
  assert(::mlir::succeeded(_result_mesh));
  assert(::mlir::succeeded(_result_split_axes));
  return odsParser.getChecked<MeshShardingAttr>(odsLoc, odsParser.getContext(),
      ::mlir::FlatSymbolRefAttr((*_result_mesh)),
      ::llvm::ArrayRef<MeshAxesAttr>((*_result_split_axes)),
      ::llvm::ArrayRef<MeshAxis>((_result_partial_axes.value_or(::llvm::SmallVector<MeshAxis>()))),
      ::mlir::mesh::ReductionKind((_result_partial_type.value_or(::mlir::mesh::ReductionKind()))));
}

void MeshShardingAttr::print(::mlir::AsmPrinter &odsPrinter) const {
  ::mlir::Builder odsBuilder(getContext());
  odsPrinter << "<";
  odsPrinter.printStrippedAttrOrType(getMesh());
  odsPrinter << ",";
  odsPrinter << ' ' << "[";
  odsPrinter.printStrippedAttrOrType(getSplitAxes());
  odsPrinter << "]";
  if (!(::llvm::ArrayRef<MeshAxis>(getPartialAxes()) == ::llvm::ArrayRef<MeshAxis>(::llvm::SmallVector<MeshAxis>()))) {
    odsPrinter << ",";
    odsPrinter << ' ' << "partial";
    odsPrinter << ' ' << "=";
    if (!(getPartialType() == ::mlir::mesh::ReductionKind())) {
      odsPrinter << ' ';
      odsPrinter.printStrippedAttrOrType(getPartialType());
    }
    odsPrinter << "[";
    if (!(::llvm::ArrayRef<MeshAxis>(getPartialAxes()) == ::llvm::ArrayRef<MeshAxis>(::llvm::SmallVector<MeshAxis>()))) {
      odsPrinter.printStrippedAttrOrType(getPartialAxes());
    }
    odsPrinter << "]";
  } else {
  }
  odsPrinter << ">";
}

::mlir::FlatSymbolRefAttr MeshShardingAttr::getMesh() const {
  return getImpl()->mesh;
}

::llvm::ArrayRef<MeshAxesAttr> MeshShardingAttr::getSplitAxes() const {
  return getImpl()->split_axes;
}

::llvm::ArrayRef<MeshAxis> MeshShardingAttr::getPartialAxes() const {
  return getImpl()->partial_axes;
}

::mlir::mesh::ReductionKind MeshShardingAttr::getPartialType() const {
  return getImpl()->partial_type;
}

} // namespace mesh
} // namespace mlir
MLIR_DEFINE_EXPLICIT_TYPE_ID(::mlir::mesh::MeshShardingAttr)
namespace mlir {
namespace mesh {

/// Parse an attribute registered to this dialect.
::mlir::Attribute MeshDialect::parseAttribute(::mlir::DialectAsmParser &parser,
                                      ::mlir::Type type) const {
  ::llvm::SMLoc typeLoc = parser.getCurrentLocation();
  ::llvm::StringRef attrTag;
  {
    ::mlir::Attribute attr;
    auto parseResult = generatedAttributeParser(parser, &attrTag, type, attr);
    if (parseResult.has_value())
      return attr;
  }
  
  parser.emitError(typeLoc) << "unknown attribute `"
      << attrTag << "` in dialect `" << getNamespace() << "`";
  return {};
}
/// Print an attribute registered to this dialect.
void MeshDialect::printAttribute(::mlir::Attribute attr,
                         ::mlir::DialectAsmPrinter &printer) const {
  if (::mlir::succeeded(generatedAttributePrinter(attr, printer)))
    return;
  
}
} // namespace mesh
} // namespace mlir

#endif  // GET_ATTRDEF_CLASSES

