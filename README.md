# Home Audio FastAPI Backend with Supabase Integration

A complete FastAPI backend for a home audio streaming platform with Supabase authentication and JWT verification.

## Features

- **Complete CRUD operations** for all entities
- **Supabase integration** for user authentication
- **JWT verification** for protected endpoints
- **User-specific features** with proper authorization
- **SQLAlchemy ORM** with proper relationships
- **Pydantic schemas** for request/response validation
- **UUID primary keys** throughout
- **Support for both PostgreSQL and SQLite** databases
- **Graceful error handling** and logging

## Quick Start

### 1. Clone and Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Update the `.env` file with your configuration:

```env
# Database Configuration (SQLite for development)
DATABASE_URL=sqlite:///./home_audio.db

# Application Configuration
SECRET_KEY=your-secret-key-change-this-in-production
DEBUG=True

# Supabase Configuration (Required for authentication)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

# JWT Configuration
JWT_SECRET_KEY=your-jwt-secret-key
JWT_ALGORITHM=HS256
JWT_ACCESS_TOKEN_EXPIRE_MINUTES=30
```

### 3. Set Up Supabase (Optional but Recommended)

1. Create a [Supabase](https://supabase.com) project
2. Get your project URL and API keys from the Supabase dashboard
3. Update the `.env` file with your Supabase credentials

### 4. Run the Application

```bash
uvicorn app.main:app --reload
```

The API will be available at:
- **API**: http://localhost:8000
- **Documentation**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

## Authentication

The API uses JWT tokens for authentication with optional Supabase integration:

### Getting a Token (Development)

```bash
curl -X POST "http://localhost:8000/api/v1/auth/create-token" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "123e4567-e89b-12d3-a456-426614174000",
    "email": "user@example.com"
  }'
```

### Using the Token

Include the token in the Authorization header:

```bash
curl -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  http://localhost:8000/api/v1/continue-watching/my-list
```

## API Endpoints

### Authentication
- `POST /api/v1/auth/create-token` - Create JWT token (development)
- `GET /api/v1/auth/me` - Get current user info
- `POST /api/v1/auth/verify-token` - Verify token validity
- `GET /api/v1/auth/supabase-status` - Check Supabase connection

### Public Endpoints (No Authentication Required)
- `GET /api/v1/stories/` - List all stories
- `GET /api/v1/episodes/` - List all episodes
- `GET /api/v1/home-content/` - List content categories
- `GET /api/v1/home-slideshow/` - List slideshow items

### Protected Endpoints (Authentication Required)

#### Continue Watching
- `GET /api/v1/continue-watching/my-list` - Get user's continue watching list
- `POST /api/v1/continue-watching/update-progress` - Update viewing progress
- `POST /api/v1/continue-watching/mark-completed/{episode_id}` - Mark episode as completed

#### Comments
- `POST /api/v1/comments/` - Create comment
- `GET /api/v1/comments/my-comments` - Get user's comments
- `PATCH /api/v1/comments/{comment_id}` - Update own comment
- `DELETE /api/v1/comments/{comment_id}` - Delete own comment

#### Likes
- `POST /api/v1/likes/story/{story_id}/toggle` - Toggle story like
- `POST /api/v1/likes/episode/{episode_id}/toggle` - Toggle episode like
- `GET /api/v1/likes/my-likes` - Get user's likes

#### Ratings
- `POST /api/v1/ratings/story/{story_id}/upsert` - Rate a story
- `POST /api/v1/ratings/episode/{episode_id}/upsert` - Rate an episode
- `GET /api/v1/ratings/my-ratings` - Get user's ratings

## Database Schema

The project includes 10 main entities:
- **Stories** (main series/shows)
- **Episodes** 
- **Comments** (with user authentication)
- **Likes** (with user authentication)
- **Ratings** (with user authentication)
- **Views**
- **Home Content** (categories)
- **Home Content Series** 
- **Home Slideshow**
- **Continue Watching** (with user authentication)

## Project Structure

```
home_audio/
├── app/
│   ├── auth/                 # Authentication modules
│   │   ├── __init__.py
│   │   ├── jwt_handler.py    # JWT token handling
│   │   ├── supabase_client.py # Supabase integration
│   │   └── dependencies.py   # Auth dependencies
│   ├── models/               # SQLAlchemy ORM models
│   ├── services/             # Business logic & CRUD operations
│   ├── routes/               # FastAPI route handlers
│   ├── config.py             # Application configuration
│   ├── database.py           # Database connection setup
│   └── main.py               # FastAPI application entrypoint
├── .env                      # Environment variables
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation
```

## Authentication Flow

1. **User Registration/Login**: Handle through Supabase Auth or your frontend
2. **Token Creation**: Use `/api/v1/auth/create-token` for development
3. **Protected Requests**: Include JWT token in Authorization header
4. **Token Verification**: Automatic verification for protected endpoints
5. **User Context**: Access current user info in protected route handlers

## Key Features

- **JWT Authentication** with optional Supabase integration
- **User-specific endpoints** with proper authorization
- **Owner-only access** for user-generated content (comments, likes, ratings)
- **Progress tracking** for episodes with continue watching
- **Toggle functionality** for likes
- **Upsert operations** for ratings
- **Graceful degradation** when Supabase is not configured

## Security

- **JWT tokens** with configurable expiration
- **User ownership validation** for user-generated content
- **Optional Supabase user verification**
- **CORS middleware** (configure for production)
- **Environment-based configuration**

## Development

To run in development mode with detailed logging:

```bash
uvicorn app.main:app --reload --log-level debug
```

## Testing Authentication

1. Create a test token:
```bash
curl -X POST "http://localhost:8000/api/v1/auth/create-token" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "test-user-123", "email": "test@example.com"}'
```

2. Use the token for protected endpoints:
```bash
curl -H "Authorization: Bearer YOUR_TOKEN" \
  "http://localhost:8000/api/v1/continue-watching/my-list"
```

## Production Deployment

1. Set up PostgreSQL database
2. Configure Supabase project
3. Update environment variables
4. Set strong JWT secret keys
5. Configure CORS origins properly
6. Use HTTPS in production