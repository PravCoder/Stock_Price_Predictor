/*===- TableGen'erated file -------------------------------------*- C++ -*-===*\
|*                                                                            *|
|* Interface Declarations                                                     *|
|*                                                                            *|
|* Automatically generated file, do not edit!                                 *|
|*                                                                            *|
\*===----------------------------------------------------------------------===*/

namespace mlir {
namespace gpu {
class TargetAttrInterface;
namespace detail {
struct TargetAttrInterfaceInterfaceTraits {
  struct Concept {
    /// The methods defined by the interface.
    std::optional<SmallVector<char, 0>> (*serializeToObject)(const Concept *impl, ::mlir::Attribute , Operation*, const gpu::TargetOptions&);
    Attribute (*createObject)(const Concept *impl, ::mlir::Attribute , const SmallVector<char, 0>&, const gpu::TargetOptions&);
  };
  template<typename ConcreteAttr>
  class Model : public Concept {
  public:
    using Interface = ::mlir::gpu::TargetAttrInterface;
    Model() : Concept{serializeToObject, createObject} {}

    static inline std::optional<SmallVector<char, 0>> serializeToObject(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, Operation* module, const gpu::TargetOptions& options);
    static inline Attribute createObject(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, const SmallVector<char, 0>& object, const gpu::TargetOptions& options);
  };
  template<typename ConcreteAttr>
  class FallbackModel : public Concept {
  public:
    using Interface = ::mlir::gpu::TargetAttrInterface;
    FallbackModel() : Concept{serializeToObject, createObject} {}

    static inline std::optional<SmallVector<char, 0>> serializeToObject(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, Operation* module, const gpu::TargetOptions& options);
    static inline Attribute createObject(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, const SmallVector<char, 0>& object, const gpu::TargetOptions& options);
  };
  template<typename ConcreteModel, typename ConcreteAttr>
  class ExternalModel : public FallbackModel<ConcreteModel> {
  public:
    using ConcreteEntity = ConcreteAttr;
  };
};
template <typename ConcreteAttr>
struct TargetAttrInterfaceTrait;

} // namespace detail
class TargetAttrInterface : public ::mlir::AttributeInterface<TargetAttrInterface, detail::TargetAttrInterfaceInterfaceTraits> {
public:
  using ::mlir::AttributeInterface<TargetAttrInterface, detail::TargetAttrInterfaceInterfaceTraits>::AttributeInterface;
  template <typename ConcreteAttr>
  struct Trait : public detail::TargetAttrInterfaceTrait<ConcreteAttr> {};
  /// Serializes a GPU module to a string containing a representation of the
  /// module.
  /// 
  /// If serialization fails then the method should return `std::nullopt`.
  /// 
  /// The `module` parameter must be a GPU Module Op. The `options` parameter
  /// is meant to be used for passing additional options that are not in the
  /// attribute.
  std::optional<SmallVector<char, 0>> serializeToObject(Operation* module, const gpu::TargetOptions& options) const;
  /// Creates a GPU object attribute from a binary string.
  /// 
  /// The `object` parameter is a binary string. The `options` parameter is
  /// meant to be used for passing additional options that are not in the
  /// attribute.
  Attribute createObject(const SmallVector<char, 0>& object, const gpu::TargetOptions& options) const;
};
namespace detail {
  template <typename ConcreteAttr>
  struct TargetAttrInterfaceTrait : public ::mlir::AttributeInterface<TargetAttrInterface, detail::TargetAttrInterfaceInterfaceTraits>::Trait<ConcreteAttr> {
  };
}// namespace detail
} // namespace gpu
} // namespace mlir
namespace mlir {
namespace gpu {
class OffloadingLLVMTranslationAttrInterface;
namespace detail {
struct OffloadingLLVMTranslationAttrInterfaceInterfaceTraits {
  struct Concept {
    /// The methods defined by the interface.
    LogicalResult (*embedBinary)(const Concept *impl, ::mlir::Attribute , Operation*, llvm::IRBuilderBase&, LLVM::ModuleTranslation&);
    LogicalResult (*launchKernel)(const Concept *impl, ::mlir::Attribute , Operation*, Operation*, llvm::IRBuilderBase&, LLVM::ModuleTranslation&);
  };
  template<typename ConcreteAttr>
  class Model : public Concept {
  public:
    using Interface = ::mlir::gpu::OffloadingLLVMTranslationAttrInterface;
    Model() : Concept{embedBinary, launchKernel} {}

    static inline LogicalResult embedBinary(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, Operation* binaryOp, llvm::IRBuilderBase& hostBuilder, LLVM::ModuleTranslation& hostModuleTranslation);
    static inline LogicalResult launchKernel(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, Operation* launchFunc, Operation* binaryOp, llvm::IRBuilderBase& hostBuilder, LLVM::ModuleTranslation& hostModuleTranslation);
  };
  template<typename ConcreteAttr>
  class FallbackModel : public Concept {
  public:
    using Interface = ::mlir::gpu::OffloadingLLVMTranslationAttrInterface;
    FallbackModel() : Concept{embedBinary, launchKernel} {}

    static inline LogicalResult embedBinary(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, Operation* binaryOp, llvm::IRBuilderBase& hostBuilder, LLVM::ModuleTranslation& hostModuleTranslation);
    static inline LogicalResult launchKernel(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, Operation* launchFunc, Operation* binaryOp, llvm::IRBuilderBase& hostBuilder, LLVM::ModuleTranslation& hostModuleTranslation);
  };
  template<typename ConcreteModel, typename ConcreteAttr>
  class ExternalModel : public FallbackModel<ConcreteModel> {
  public:
    using ConcreteEntity = ConcreteAttr;
  };
};
template <typename ConcreteAttr>
struct OffloadingLLVMTranslationAttrInterfaceTrait;

} // namespace detail
class OffloadingLLVMTranslationAttrInterface : public ::mlir::AttributeInterface<OffloadingLLVMTranslationAttrInterface, detail::OffloadingLLVMTranslationAttrInterfaceInterfaceTraits> {
public:
  using ::mlir::AttributeInterface<OffloadingLLVMTranslationAttrInterface, detail::OffloadingLLVMTranslationAttrInterfaceInterfaceTraits>::AttributeInterface;
  template <typename ConcreteAttr>
  struct Trait : public detail::OffloadingLLVMTranslationAttrInterfaceTrait<ConcreteAttr> {};
  /// Translates a `gpu.binary` Op into a sequence of LLVM IR target-specific
  /// instructions, embedding the binary into a host LLVM module.
  /// 
  /// The LLVM translation mechanism invokes this function when translating a
  /// `gpu.binary`.
  /// 
  /// The first argument has to be a GPU binary operation.
  /// If the function fails at any point, it must return `failure`.
  LogicalResult embedBinary(Operation* binaryOp, llvm::IRBuilderBase& hostBuilder, LLVM::ModuleTranslation& hostModuleTranslation) const;
  /// Translates a `gpu.launch_func` op into a sequence of LLVM IR
  /// target-specific instructions, resulting in a kernel launch on host IR.
  /// 
  /// The LLVM translation mechanism invokes this function when translating a
  /// `gpu.launch_func` operation; it searches the appropriate binary and uses
  /// its offloading handler.
  /// 
  /// The first two arguments must be GPU launch and binary operations,
  /// respectively. If the function fails at any point, it must return
  /// `failure`.
  LogicalResult launchKernel(Operation* launchFunc, Operation* binaryOp, llvm::IRBuilderBase& hostBuilder, LLVM::ModuleTranslation& hostModuleTranslation) const;
};
namespace detail {
  template <typename ConcreteAttr>
  struct OffloadingLLVMTranslationAttrInterfaceTrait : public ::mlir::AttributeInterface<OffloadingLLVMTranslationAttrInterface, detail::OffloadingLLVMTranslationAttrInterfaceInterfaceTraits>::Trait<ConcreteAttr> {
  };
}// namespace detail
} // namespace gpu
} // namespace mlir
namespace mlir {
namespace gpu {
template<typename ConcreteAttr>
std::optional<SmallVector<char, 0>> detail::TargetAttrInterfaceInterfaceTraits::Model<ConcreteAttr>::serializeToObject(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, Operation* module, const gpu::TargetOptions& options) {
  return (::llvm::cast<ConcreteAttr>(tablegen_opaque_val)).serializeToObject(module, options);
}
template<typename ConcreteAttr>
Attribute detail::TargetAttrInterfaceInterfaceTraits::Model<ConcreteAttr>::createObject(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, const SmallVector<char, 0>& object, const gpu::TargetOptions& options) {
  return (::llvm::cast<ConcreteAttr>(tablegen_opaque_val)).createObject(object, options);
}
template<typename ConcreteAttr>
std::optional<SmallVector<char, 0>> detail::TargetAttrInterfaceInterfaceTraits::FallbackModel<ConcreteAttr>::serializeToObject(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, Operation* module, const gpu::TargetOptions& options) {
  return static_cast<const ConcreteAttr *>(impl)->serializeToObject(tablegen_opaque_val, module, options);
}
template<typename ConcreteAttr>
Attribute detail::TargetAttrInterfaceInterfaceTraits::FallbackModel<ConcreteAttr>::createObject(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, const SmallVector<char, 0>& object, const gpu::TargetOptions& options) {
  return static_cast<const ConcreteAttr *>(impl)->createObject(tablegen_opaque_val, object, options);
}
} // namespace gpu
} // namespace mlir
namespace mlir {
namespace gpu {
template<typename ConcreteAttr>
LogicalResult detail::OffloadingLLVMTranslationAttrInterfaceInterfaceTraits::Model<ConcreteAttr>::embedBinary(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, Operation* binaryOp, llvm::IRBuilderBase& hostBuilder, LLVM::ModuleTranslation& hostModuleTranslation) {
  return (::llvm::cast<ConcreteAttr>(tablegen_opaque_val)).embedBinary(binaryOp, hostBuilder, hostModuleTranslation);
}
template<typename ConcreteAttr>
LogicalResult detail::OffloadingLLVMTranslationAttrInterfaceInterfaceTraits::Model<ConcreteAttr>::launchKernel(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, Operation* launchFunc, Operation* binaryOp, llvm::IRBuilderBase& hostBuilder, LLVM::ModuleTranslation& hostModuleTranslation) {
  return (::llvm::cast<ConcreteAttr>(tablegen_opaque_val)).launchKernel(launchFunc, binaryOp, hostBuilder, hostModuleTranslation);
}
template<typename ConcreteAttr>
LogicalResult detail::OffloadingLLVMTranslationAttrInterfaceInterfaceTraits::FallbackModel<ConcreteAttr>::embedBinary(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, Operation* binaryOp, llvm::IRBuilderBase& hostBuilder, LLVM::ModuleTranslation& hostModuleTranslation) {
  return static_cast<const ConcreteAttr *>(impl)->embedBinary(tablegen_opaque_val, binaryOp, hostBuilder, hostModuleTranslation);
}
template<typename ConcreteAttr>
LogicalResult detail::OffloadingLLVMTranslationAttrInterfaceInterfaceTraits::FallbackModel<ConcreteAttr>::launchKernel(const Concept *impl, ::mlir::Attribute tablegen_opaque_val, Operation* launchFunc, Operation* binaryOp, llvm::IRBuilderBase& hostBuilder, LLVM::ModuleTranslation& hostModuleTranslation) {
  return static_cast<const ConcreteAttr *>(impl)->launchKernel(tablegen_opaque_val, launchFunc, binaryOp, hostBuilder, hostModuleTranslation);
}
} // namespace gpu
} // namespace mlir
