declare module "d3-sankey" {
  export interface SankeyNode<N, L> {
    name: string;
    x0?: number;
    x1?: number;
    y0?: number;
    y1?: number;
    value?: number;
    index?: number;
    sourceLinks: SankeyLink<N, L>[];
    targetLinks: SankeyLink<N, L>[];
  }

  export interface SankeyLink<N, L> {
    source: SankeyNode<N, L> | number;
    target: SankeyNode<N, L> | number;
    value: number;
    width?: number;
    y0?: number;
    y1?: number;
  }

  export interface SankeyLayout<N, L> {
    (graph: { nodes: N[]; links: L[] }): { nodes: SankeyNode<N, L>[]; links: SankeyLink<N, L>[] };
    nodeAlign(align: (node: SankeyNode<N, L>, n: number) => number): this;
    nodeWidth(width: number): this;
    nodePadding(padding: number): this;
    extent(extent: [[number, number], [number, number]]): this;
    size(size: [number, number]): this;
  }

  export function sankey<N, L>(): SankeyLayout<N, L>;
  export function sankeyLinkHorizontal(): (link: any) => string | null;
  export function sankeyJustify(node: any, n: number): number;
  export function sankeyLeft(node: any, n: number): number;
  export function sankeyRight(node: any, n: number): number;
  export function sankeyCenter(node: any, n: number): number;
}
