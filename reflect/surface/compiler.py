# reflect.surface.compiler
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List, Any
from plane.emitter import get_logger

log = get_logger("surface.compiler")

## @archetype
T_Node = TypeVar('T_Node')       ## Φ: topology node (observed unit)
T_Rep = TypeVar('T_Rep')         ## R: projected representation
T_Surface = TypeVar('T_Surface') ## Φs: assembled surface structure

class SurfaceCompiler(ABC, Generic[T_Node, T_Rep, T_Surface]):
    """Abstract pipeline projecting topology (Φ) into surface structure (Φs)."""
    
    def compile(self) -> None:
        log.info("[AUG] Surface Compile")
        
        topology: List[T_Node] = self.scan()
        log.info(f"## @scan: {len(topology)} items found.")
        
        skeleton: List[T_Node] = self.filter(topology)
        log.info(f"## @bound (Filtered): {len(skeleton)} items valid.")
        
        representations: List[T_Rep] = self.project(skeleton)
        log.info(f"## @extract (Projected): {len(representations)} items structured.")
        
        surface: T_Surface = self.assemble(representations)
        log.info(f"## @group (Assembled): {len(surface)} groups created.")
        
        self.emit(surface)
        log.info("## @emit: Surface projection completed.")
        log.info("[UGA] compile completed")

    @abstractmethod
    def scan(self) -> List[T_Node]:
        ## Φ observation: collect topology nodes
        pass

    @abstractmethod
    def filter(self, topology: List[T_Node]) -> List[T_Node]:
        ## Φ → Φ′ boundary filtering
        pass

    @abstractmethod
    def project(self, skeleton: List[T_Node]) -> List[T_Rep]:
        ## Φ′ → R structural projection
        pass

    @abstractmethod
    def assemble(self, representations: List[T_Rep]) -> T_Surface:
        ## R alignment -> Φs surface assemble
        pass

    @abstractmethod
    def emit(self, surface: T_Surface) -> None:
        ## Φs materialization
        pass