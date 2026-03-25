// API configuration for production deployment
// Uses production URL by default, override with VITE_API_URL for local development
const API_BASE = import.meta.env.VITE_API_URL || 'https://conduit-uzto.onrender.com'

export default API_BASE
