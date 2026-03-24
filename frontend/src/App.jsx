import { useEffect, useState, useCallback, useRef, useMemo } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import ReactMarkdown from 'react-markdown'
import axios from 'axios'
import './App.css'

const API_URL = 'http://localhost:8000'

const NODE_COLORS = {
  SalesOrder: '#4A90D9',
  SalesOrderItem: '#5BA3EC',
  DeliveryHeader: '#6BB8A0',
  DeliveryItem: '#7FCAB3',
  BillingHeader: '#E07A5F',
  BillingItem: '#F09A7F',
  JournalEntry: '#D4A574',
  Payment: '#81B29A',
  BusinessPartner: '#9B8AC4',
  Product: '#F2CC8F',
  Plant: '#A8DADC',
}

// Group positions for initial layout clustering
const TYPE_POSITIONS = {
  SalesOrder: { x: 0, y: -200 },
  SalesOrderItem: { x: 100, y: -100 },
  DeliveryHeader: { x: -200, y: 0 },
  DeliveryItem: { x: -100, y: 100 },
  BillingHeader: { x: 200, y: 0 },
  BillingItem: { x: 300, y: 100 },
  JournalEntry: { x: 200, y: 200 },
  Payment: { x: 100, y: 300 },
  BusinessPartner: { x: -200, y: -200 },
  Product: { x: 0, y: 100 },
  Plant: { x: -300, y: 100 },
}

// Extract entity IDs from LLM response text
function extractEntityIds(text) {
  const ids = new Set()
  // Match common patterns: numbers 6+ digits, or patterns like "740506"
  const patterns = [
    /\b(\d{6,})\b/g,  // 6+ digit numbers (order IDs, billing docs, etc.)
    /\b([A-Z]{2,}\d{3,})\b/g,  // Alphanumeric like "OR740506"
  ]
  for (const pattern of patterns) {
    const matches = text.matchAll(pattern)
    for (const match of matches) {
      ids.add(match[1])
    }
  }
  return ids
}

function App() {
  const [graphData, setGraphData] = useState({ nodes: [], links: [] })
  const [selectedNode, setSelectedNode] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [stats, setStats] = useState(null)
  const [searchQuery, setSearchQuery] = useState('')
  const [highlightedIds, setHighlightedIds] = useState(new Set())
  const graphRef = useRef()

  // Chat state
  const [messages, setMessages] = useState([])
  const [inputValue, setInputValue] = useState('')
  const [chatLoading, setChatLoading] = useState(false)
  const messagesEndRef = useRef(null)

  // Fetch graph and stats on load
  useEffect(() => {
    const fetchData = async () => {
      try {
        const [graphRes, statsRes] = await Promise.all([
          axios.get(`${API_URL}/api/graph`),
          axios.get(`${API_URL}/api/stats`),
        ])

        const { nodes, edges } = graphRes.data

        // Add initial positions based on type for clustering
        const processedNodes = nodes.map((n, i) => {
          const basePos = TYPE_POSITIONS[n.type] || { x: 0, y: 0 }
          const spread = 50
          return {
            ...n,
            color: NODE_COLORS[n.type] || '#888',
            x: basePos.x + (Math.random() - 0.5) * spread,
            y: basePos.y + (Math.random() - 0.5) * spread,
          }
        })

        setGraphData({
          nodes: processedNodes,
          links: edges.map(e => ({
            source: e.source,
            target: e.target,
            relationship: e.relationship,
          })),
        })
        setStats(statsRes.data)
        setLoading(false)
      } catch (err) {
        setError(err.message)
        setLoading(false)
      }
    }
    fetchData()
  }, [])

  // Auto-scroll chat
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  // Clear highlights after 5 seconds
  useEffect(() => {
    if (highlightedIds.size > 0) {
      const timer = setTimeout(() => setHighlightedIds(new Set()), 5000)
      return () => clearTimeout(timer)
    }
  }, [highlightedIds])

  // Filter nodes based on search
  const filteredNodes = useMemo(() => {
    if (!searchQuery.trim()) return new Set()
    const query = searchQuery.toLowerCase()
    return new Set(
      graphData.nodes
        .filter(n =>
          n.label.toLowerCase().includes(query) ||
          n.type.toLowerCase().includes(query)
        )
        .map(n => n.id)
    )
  }, [searchQuery, graphData.nodes])

  const handleNodeClick = useCallback((node) => {
    setSelectedNode(node)
  }, [])

  const handleCanvasClick = useCallback((event) => {
    if (event.target.tagName === 'CANVAS') {
      setSelectedNode(null)
    }
  }, [])

  const nodeCanvasObject = useCallback((node, ctx, globalScale) => {
    const label = node.label
    const fontSize = 12 / globalScale
    ctx.font = `${fontSize}px Inter, system-ui, sans-serif`

    const isSelected = selectedNode && selectedNode.id === node.id
    const isHighlighted = highlightedIds.has(node.label) || highlightedIds.has(node.id)
    const isSearchMatch = filteredNodes.has(node.id)
    const isFiltering = searchQuery.trim().length > 0
    const isDimmed = isFiltering && !isSearchMatch

    // Draw glow for highlighted nodes
    if (isHighlighted) {
      const gradient = ctx.createRadialGradient(node.x, node.y, 0, node.x, node.y, 20)
      gradient.addColorStop(0, node.color)
      gradient.addColorStop(1, 'transparent')
      ctx.fillStyle = gradient
      ctx.beginPath()
      ctx.arc(node.x, node.y, 20, 0, 2 * Math.PI)
      ctx.fill()
    }

    // Draw node circle
    ctx.beginPath()
    ctx.arc(node.x, node.y, isHighlighted ? 8 : 6, 0, 2 * Math.PI)
    ctx.fillStyle = isDimmed ? 'rgba(100, 100, 100, 0.3)' : node.color
    ctx.fill()

    // Draw border for selected or search matched
    if (isSelected || isSearchMatch) {
      ctx.strokeStyle = isSelected ? '#fff' : '#ffcc00'
      ctx.lineWidth = 2 / globalScale
      ctx.stroke()
    }

    // Draw label
    ctx.fillStyle = isDimmed ? 'rgba(255, 255, 255, 0.2)' : 'rgba(255, 255, 255, 0.85)'
    ctx.textAlign = 'center'
    ctx.textBaseline = 'top'
    ctx.fillText(label, node.x, node.y + (isHighlighted ? 10 : 8))
  }, [selectedNode, highlightedIds, filteredNodes, searchQuery])

  const sendMessage = async () => {
    if (!inputValue.trim() || chatLoading) return

    const userMessage = inputValue.trim()
    setInputValue('')

    const newMessages = [...messages, { role: 'user', content: userMessage }]
    setMessages(newMessages)
    setChatLoading(true)

    try {
      const response = await axios.post(`${API_URL}/api/chat`, {
        message: userMessage,
        history: newMessages.slice(0, -1),
      })

      const assistantMessage = response.data.response

      setMessages([
        ...newMessages,
        {
          role: 'assistant',
          content: assistantMessage,
          query_used: response.data.query_used,
        },
      ])

      // Highlight mentioned entity IDs in graph
      const mentionedIds = extractEntityIds(assistantMessage)
      if (mentionedIds.size > 0) {
        setHighlightedIds(mentionedIds)
      }
    } catch (err) {
      setMessages([
        ...newMessages,
        { role: 'assistant', content: `Error: ${err.message}` },
      ])
    } finally {
      setChatLoading(false)
    }
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage()
    }
  }

  if (loading) {
    return (
      <div className="app">
        <div className="loading">Loading graph...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="app">
        <div className="error">Error: {error}</div>
      </div>
    )
  }

  return (
    <div className="app" onClick={handleCanvasClick}>
      {/* Stats Bar */}
      <div className="stats-bar">
        <span className="stats-item">{stats?.total_nodes || 0} nodes</span>
        <span className="stats-divider">·</span>
        <span className="stats-item">{stats?.total_edges || 0} edges</span>
        <span className="stats-divider">·</span>
        <span className="stats-item">{stats?.entity_types || 0} entity types</span>
      </div>

      <div className="main-content">
        <div className="graph-container">
          {/* Search Bar */}
          <div className="search-bar">
            <input
              type="text"
              placeholder="Search nodes..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="search-input"
            />
            {searchQuery && (
              <span className="search-count">
                {filteredNodes.size} match{filteredNodes.size !== 1 ? 'es' : ''}
              </span>
            )}
          </div>

          <ForceGraph2D
            ref={graphRef}
            graphData={graphData}
            nodeId="id"
            nodeLabel=""
            nodeCanvasObject={nodeCanvasObject}
            nodePointerAreaPaint={(node, color, ctx) => {
              ctx.beginPath()
              ctx.arc(node.x, node.y, 8, 0, 2 * Math.PI)
              ctx.fillStyle = color
              ctx.fill()
            }}
            linkColor={() => 'rgba(255, 255, 255, 0.12)'}
            linkWidth={1}
            onNodeClick={handleNodeClick}
            backgroundColor="#0f1117"
            cooldownTicks={150}
            d3AlphaDecay={0.02}
            d3VelocityDecay={0.3}
            onEngineStop={() => graphRef.current?.zoomToFit(400, 60)}
          />

          <div className="legend">
            <div className="legend-title">Entity Types</div>
            {Object.entries(NODE_COLORS).map(([type, color]) => (
              <div key={type} className="legend-item">
                <span className="legend-dot" style={{ backgroundColor: color }} />
                <span className="legend-label">{type}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="side-panel">
          <div className="panel-header">
            <h1>Order to Cash Explorer</h1>
          </div>

          {/* Node Info Panel */}
          <div className="info-panel">
            {selectedNode ? (
              <div className="node-details">
                <div className="node-type" style={{ color: NODE_COLORS[selectedNode.type] }}>
                  {selectedNode.type}
                </div>
                <div className="node-id">{selectedNode.label}</div>

                <div className="node-fields">
                  {Object.entries(selectedNode.data).map(([key, value]) => (
                    <div key={key} className="field-row">
                      <span className="field-key">{key}</span>
                      <span className="field-value">{value || '—'}</span>
                    </div>
                  ))}
                </div>
              </div>
            ) : (
              <div className="no-selection">
                <p>Click a node to view details</p>
                <div className="mini-stats">
                  {stats?.by_type && Object.entries(stats.by_type).slice(0, 4).map(([type, count]) => (
                    <div key={type} className="mini-stat">
                      <span className="mini-stat-dot" style={{ backgroundColor: NODE_COLORS[type] }} />
                      <span>{count}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Chat Panel */}
          <div className="chat-panel">
            <div className="chat-header">
              <div className="chat-title">Chat with Graph</div>
              <div className="chat-subtitle">SAP Order-to-Cash Dataset</div>
            </div>

            <div className="messages-container">
              {messages.length === 0 ? (
                <div className="chat-empty">
                  <p>Ask questions about your O2C data</p>
                  <div className="chat-suggestions">
                    <button onClick={() => setInputValue('How many sales orders are there?')}>
                      How many sales orders?
                    </button>
                    <button onClick={() => setInputValue('Show top 5 customers by order value')}>
                      Top 5 customers
                    </button>
                  </div>
                </div>
              ) : (
                messages.map((msg, idx) => (
                  <div key={idx} className={`message ${msg.role}`}>
                    <div className="message-bubble">
                      {msg.role === 'assistant' ? (
                        <ReactMarkdown>{msg.content}</ReactMarkdown>
                      ) : (
                        msg.content
                      )}
                    </div>
                  </div>
                ))
              )}
              {chatLoading && (
                <div className="message assistant">
                  <div className="message-bubble loading-bubble">
                    <span className="dot"></span>
                    <span className="dot"></span>
                    <span className="dot"></span>
                  </div>
                </div>
              )}
              <div ref={messagesEndRef} />
            </div>

            <div className="chat-input-container">
              <input
                type="text"
                className="chat-input"
                placeholder="Ask about orders, deliveries, payments..."
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
                onKeyDown={handleKeyDown}
                disabled={chatLoading}
              />
              <button
                className="send-button"
                onClick={sendMessage}
                disabled={chatLoading || !inputValue.trim()}
              >
                Send
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

export default App
